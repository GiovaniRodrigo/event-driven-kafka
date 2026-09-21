import { Kafka, Consumer, Producer, EachMessagePayload } from 'kafkajs';
import { kafkaConfig, consumerConfig, producerConfig, topics } from '../config';
import { logger } from '../utils/logger';
import { DatabaseService } from '../services/database';
import { EventEnvelope, createEventEnvelope, validateEventEnvelope } from '../contracts/envelope';
import { v4 as uuidv4 } from 'uuid';

export type ConsumerHealth = 'healthy' | 'degraded' | 'down';

export abstract class BaseConsumer {
  protected consumer: Consumer;
  protected producer: Producer;
  protected kafka: Kafka;
  protected db: DatabaseService;
  protected maxRetries = 3;
  protected baseBackoffMs = 500;
  protected jitterMs = 200;
  protected status: ConsumerHealth = 'down';
  protected consumerName: string;
  protected lastEventTimestamp?: string;
  protected lastProcessedTimestamp?: string;
  protected processedCount = 0;
  protected failureCount = 0;

  constructor(
    protected topicName: string | string[],
    protected groupId: string,
    consumerName?: string,
    dbInstance?: DatabaseService
  ) {
    this.consumerName = consumerName || groupId;
    this.kafka = new Kafka(kafkaConfig);
    this.consumer = this.kafka.consumer({
      ...consumerConfig,
      groupId: this.groupId,
    });
    this.producer = this.kafka.producer(producerConfig);
    this.db = dbInstance || new DatabaseService();
  }

  get health(): ConsumerHealth {
    return this.status;
  }

  get metrics() {
    return {
      consumer: this.consumerName,
      group: this.groupId,
      status: this.status,
      processed: this.processedCount,
      failures: this.failureCount,
      lastEvent: this.lastEventTimestamp,
      lastProcessed: this.lastProcessedTimestamp,
    };
  }

  async start(): Promise<void> {
    try {
      await this.consumer.connect();
      await this.producer.connect();
      logger.info({
        event: 'consumer_connected',
        topics: this.topicName,
        group: this.groupId,
        consumer: this.consumerName,
      });

      const subscribeTopics = Array.isArray(this.topicName) ? this.topicName : [this.topicName];
      for (const t of subscribeTopics) {
        await this.consumer.subscribe({ topic: t, fromBeginning: false });
      }

      await this.consumer.run({
        eachMessage: this.handleMessage.bind(this),
      });

      this.status = 'healthy';
      logger.info({
        event: 'consumer_started',
        topics: this.topicName,
        group: this.groupId,
      });
    } catch (error) {
      this.status = 'down';
      logger.error({
        event: 'consumer_start_error',
        topics: this.topicName,
        group: this.groupId,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  /**
   * Helper to emit a strongly-typed domain event wrapped in a standard envelope.
   */
  protected async emit<T extends Record<string, unknown>>(params: {
    topic: string;
    eventType: string;
    aggregateId: string;
    aggregateType: string;
    payload: T;
    correlationId: string;
    causationId: string;
    eventId?: string;
  }): Promise<EventEnvelope<T>> {
    const envelope = createEventEnvelope<T>({
      eventId: params.eventId,
      eventType: params.eventType,
      aggregateId: params.aggregateId,
      aggregateType: params.aggregateType,
      producer: this.consumerName,
      correlationId: params.correlationId,
      causationId: params.causationId,
      payload: params.payload,
    });

    await this.producer.send({
      topic: params.topic,
      messages: [
        {
          key: params.aggregateId,
          value: JSON.stringify(envelope),
          headers: {
            'correlation-id': params.correlationId,
            'causation-id': params.causationId,
            'event-id': envelope.event_id,
            'event-type': params.eventType,
            'source-service': this.consumerName,
            timestamp: envelope.occurred_at,
          },
        },
      ],
    });

    logger.info({
      event: 'message_produced',
      topic: params.topic,
      event_type: params.eventType,
      event_id: envelope.event_id,
      aggregate_id: params.aggregateId,
      correlation_id: params.correlationId,
    });

    return envelope;
  }

  protected async handleMessage(payload: EachMessagePayload): Promise<void> {
    const startTime = Date.now();
    const { topic, partition, message } = payload;
    const offset = message.offset;
    const rawValue = message.value?.toString() || '';
    this.lastEventTimestamp = new Date().toISOString();

    let envelope: EventEnvelope;

    // 1. Parsing and Schema Validation
    try {
      const parsed = JSON.parse(rawValue);
      // Support legacy envelopes seamlessly if missing standard fields
      if (!parsed.event_id && parsed.id) parsed.event_id = parsed.id;
      if (!parsed.event_type && parsed.type) parsed.event_type = parsed.type;
      if (!parsed.aggregate_id && parsed.order_id) parsed.aggregate_id = parsed.order_id;
      if (!parsed.aggregate_type) parsed.aggregate_type = 'Order';
      if (!parsed.correlation_id && parsed.aggregate_id) parsed.correlation_id = parsed.aggregate_id;
      if (!parsed.causation_id) parsed.causation_id = parsed.event_id || `cmd_${uuidv4().slice(0, 8)}`;
      if (!parsed.occurred_at) parsed.occurred_at = new Date().toISOString();
      if (!parsed.producer) parsed.producer = 'legacy-producer';
      if (!parsed.payload) {
        const { event_id, event_type, aggregate_id, aggregate_type, correlation_id, causation_id, occurred_at, producer, ...rest } = parsed;
        parsed.payload = rest;
      }

      envelope = validateEventEnvelope(parsed);
    } catch (parseError) {
      // Non-retryable error: malformed payload -> route directly to DLQ
      logger.error({
        event: 'malformed_event_non_retryable',
        topic,
        partition,
        offset,
        error: (parseError as Error).message,
        raw_payload: rawValue.slice(0, 200),
      });

      await this.handleNonRetryableDLQ({
        raw: rawValue,
        topic,
        partition,
        offset,
        error: parseError as Error,
      });
      return;
    }

    const { event_id: eventId, correlation_id: correlationId, aggregate_id: aggregateId, event_type: eventType } = envelope;

    // 2. Robust Idempotency Check
    const alreadyProcessed = await this.db.isEventProcessed(eventId, this.consumerName);
    if (alreadyProcessed) {
      logger.info({
        event: 'consumer.duplicate',
        consumer: this.consumerName,
        event_id: eventId,
        event_type: eventType,
        aggregate_id: aggregateId,
        topic,
      });
      return;
    }

    // 3. Retry Loop with Exponential Backoff & Jitter
    let attempt = 0;
    let lastError: Error | undefined;

    while (attempt < this.maxRetries) {
      attempt++;
      try {
        if (attempt > 1) {
          logger.info({
            event: 'consumer.redelivery',
            consumer: this.consumerName,
            event_id: eventId,
            attempt,
          });
        }

        await this.processEvent(envelope);

        // Mark successfully processed in database
        await this.db.markEventProcessed(eventId, this.consumerName, 'PROCESSED');
        this.processedCount++;
        this.status = 'healthy';
        this.lastProcessedTimestamp = new Date().toISOString();

        const duration = Date.now() - startTime;
        logger.info({
          event: 'message_processed_successfully',
          consumer: this.consumerName,
          event_id: eventId,
          event_type: eventType,
          aggregate_id: aggregateId,
          correlation_id: correlationId,
          topic,
          partition,
          offset,
          attempt,
          duration_ms: duration,
        });
        return;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        logger.warn({
          event: 'consumer_process_attempt_failed',
          consumer: this.consumerName,
          event_id: eventId,
          event_type: eventType,
          attempt,
          max_retries: this.maxRetries,
          error: lastError.message,
        });

        if (attempt < this.maxRetries) {
          const delay = Math.min(10000, this.baseBackoffMs * Math.pow(2, attempt - 1)) + Math.floor(Math.random() * this.jitterMs);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    // 4. Exhausted Retries -> Send to DLQ
    this.failureCount++;
    this.status = 'degraded';
    await this.handleExhaustedRetriesDLQ({
      envelope,
      topic,
      partition,
      offset,
      error: lastError || new Error('Unknown processing error'),
      attempts: attempt,
    });
  }

  /** Concrete consumers implement domain processing */
  protected abstract processEvent(envelope: EventEnvelope): Promise<void>;

  private async handleNonRetryableDLQ(params: {
    raw: string;
    topic: string;
    partition: number;
    offset: string;
    error: Error;
  }): Promise<void> {
    const dlqId = `dlq_${uuidv4().slice(0, 8)}`;
    const dlqPayload = {
      dlq_id: dlqId,
      original_topic: params.topic,
      original_partition: params.partition,
      original_offset: params.offset,
      consumer_name: this.consumerName,
      error_message: params.error.message,
      stack_trace: params.error.stack,
      raw_message: params.raw,
      failed_at: new Date().toISOString(),
      reason: 'MALFORMED_OR_NON_RETRYABLE',
    };

    try {
      await this.db.recordDLQMessage({
        id: dlqId,
        eventId: 'malformed_event',
        topic: params.topic,
        partition: params.partition,
        offset: params.offset,
        consumerName: this.consumerName,
        errorMessage: params.error.message,
        stackTrace: params.error.stack,
        payload: dlqPayload,
      });

      logger.info({ event: 'dlq.persisted', dlq_id: dlqId, reason: 'NON_RETRYABLE' });

      await this.producer.send({
        topic: topics.dlq.name,
        messages: [{ key: dlqId, value: JSON.stringify(dlqPayload) }],
      });

      logger.info({ event: 'dlq.published', dlq_id: dlqId, topic: topics.dlq.name });
    } catch (dlqError) {
      logger.error({ event: 'dlq.save_error', error: (dlqError as Error).message });
    }
  }

  private async handleExhaustedRetriesDLQ(params: {
    envelope: EventEnvelope;
    topic: string;
    partition: number;
    offset: string;
    error: Error;
    attempts: number;
  }): Promise<void> {
    const dlqId = `dlq_${uuidv4().slice(0, 8)}`;
    const dlqPayload = {
      dlq_id: dlqId,
      original_event: params.envelope,
      original_topic: params.topic,
      original_partition: params.partition,
      original_offset: params.offset,
      consumer_name: this.consumerName,
      error_message: params.error.message,
      stack_trace: params.error.stack,
      attempts: params.attempts,
      failed_at: new Date().toISOString(),
      correlation_id: params.envelope.correlation_id,
    };

    logger.error({
      event: 'dlq_message_routed',
      dlq_id: dlqId,
      consumer: this.consumerName,
      event_id: params.envelope.event_id,
      error: params.error.message,
      attempts: params.attempts,
    });

    try {
      // 1. Record in PostgreSQL DLQ table (durably persisted)
      await this.db.recordDLQMessage({
        id: dlqId,
        eventId: params.envelope.event_id,
        topic: params.topic,
        partition: params.partition,
        offset: params.offset,
        consumerName: this.consumerName,
        errorMessage: params.error.message,
        stackTrace: params.error.stack,
        payload: dlqPayload,
        correlationId: params.envelope.correlation_id,
      });

      logger.info({ event: 'dlq.persisted', dlq_id: dlqId, event_id: params.envelope.event_id });

      // 2. Mark processed as FAILED in idempotency table so it advances the offset
      await this.db.markEventProcessed(params.envelope.event_id, this.consumerName, 'FAILED', params.error.message);

      // 3. Publish to Kafka platform.dlq topic
      await this.producer.send({
        topic: topics.dlq.name,
        messages: [
          {
            key: params.envelope.aggregate_id || dlqId,
            value: JSON.stringify(dlqPayload),
            headers: {
              'dlq-id': dlqId,
              'original-topic': params.topic,
              'correlation-id': params.envelope.correlation_id,
              'source-service': this.consumerName,
            },
          },
        ],
      });

      logger.info({ event: 'dlq.published', dlq_id: dlqId, topic: topics.dlq.name });
    } catch (dlqErr) {
      logger.error({ event: 'dlq.publish_error', error: (dlqErr as Error).message });
      throw dlqErr;
    }
  }

  async stop(): Promise<void> {
    this.status = 'down';
    try {
      await this.consumer.disconnect();
      await this.producer.disconnect();
      logger.info({
        event: 'consumer_stopped',
        topics: this.topicName,
        group: this.groupId,
        consumer: this.consumerName,
      });
    } catch (error) {
      logger.error({ event: 'consumer_stop_error', error: (error as Error).message });
    }
  }
}
