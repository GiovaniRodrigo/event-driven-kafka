import { Kafka, Consumer, Producer, EachMessagePayload } from 'kafkajs';
import { kafkaConfig, consumerConfig, producerConfig } from '../config';
import { logger } from '../utils/logger';
import { DatabaseService } from '../services/database';

export type ConsumerHealth = 'healthy' | 'degraded' | 'down';

export abstract class BaseConsumer {
  protected consumer: Consumer;
  protected producer: Producer;
  protected kafka: Kafka;
  protected db: DatabaseService;
  protected retryCount: Map<string, number> = new Map();
  protected maxRetries = 3;
  private status: ConsumerHealth = 'down';

  constructor(
    protected topicName: string,
    protected groupId: string
  ) {
    this.kafka = new Kafka(kafkaConfig);
    this.consumer = this.kafka.consumer({
      ...consumerConfig,
      groupId: this.groupId,
    });
    this.producer = this.kafka.producer(producerConfig);
    this.db = new DatabaseService();
  }

  get health(): ConsumerHealth {
    return this.status;
  }

  async start() {
    try {
      await this.consumer.connect();
      await this.producer.connect();
      logger.info({
        event: 'consumer_connected',
        topic: this.topicName,
        group: this.groupId,
      });

      await this.consumer.subscribe({
        topic: this.topicName,
        fromBeginning: false,
      });

      await this.consumer.run({
        eachMessage: this.handleMessage.bind(this),
      });

      this.status = 'healthy';
      logger.info({
        event: 'consumer_started',
        topic: this.topicName,
        group: this.groupId,
      });
    } catch (error) {
      this.status = 'down';
      logger.error({
        event: 'consumer_start_error',
        topic: this.topicName,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      process.exit(1);
    }
  }

  /** Emit a downstream event to the topic consumed by the next stage. */
  protected async emit(topic: string, eventType: string, payload: any, key?: string) {
    await this.producer.send({
      topic,
      messages: [
        {
          key: key || payload.event_id,
          value: JSON.stringify({ type: eventType, ...payload }),
          headers: {
            'correlation-id': payload.correlation_id || '',
            'event-id': payload.event_id || '',
            'source-service': this.groupId,
            timestamp: new Date().toISOString(),
          },
        },
      ],
    });
    logger.info({
      event: 'message_produced',
      topic,
      type: eventType,
      event_id: payload.event_id,
    });
  }

  protected async handleMessage(payload: EachMessagePayload) {
    const startTime = Date.now();
    const { message, partition } = payload;
    const offset = message.offset;

    let event: any;

    try {
      event = JSON.parse(message.value?.toString() || '{}');

      const eventId = event.event_id;
      const correlationId = event.correlation_id;

      const processed = await this.db.getProcessedEvent(eventId);
      if (processed) {
        logger.info({
          event: 'duplicate_event',
          event_id: eventId,
          topic: this.topicName,
        });
        return;
      }

      await this.processEvent(event);

      await this.db.markProcessed(eventId);

      this.retryCount.delete(eventId);
      this.status = 'healthy';

      const duration = Date.now() - startTime;
      logger.info({
        event: 'message_processed',
        topic: this.topicName,
        group: this.groupId,
        event_id: eventId,
        correlation_id: correlationId,
        partition,
        offset,
        duration_ms: duration,
      });
    } catch (error) {
      const eventId = event?.event_id || 'unknown';
      const currentRetry = (this.retryCount.get(eventId) || 0) + 1;
      this.retryCount.set(eventId, currentRetry);
      this.status = 'degraded';

      logger.error({
        event: 'process_error',
        topic: this.topicName,
        group: this.groupId,
        event_id: eventId,
        retry_count: currentRetry,
        max_retries: this.maxRetries,
        error: error instanceof Error ? error.message : 'Unknown error',
      });

      if (currentRetry >= this.maxRetries) {
        await this.handleError(event, error as Error);
      } else {
        throw error;
      }
    }
  }

  protected abstract processEvent(event: any): Promise<void>;

  protected async handleError(event: any, error: Error) {
    logger.error({
      event: 'dlq_send',
      event_id: event?.event_id,
      correlation_id: event?.correlation_id,
      topic: this.topicName,
      reason: error.message,
      payload: JSON.stringify(event),
    });

    try {
      await this.producer.send({
        topic: 'dlq',
        messages: [
          {
            key: event?.event_id,
            value: JSON.stringify({
              original_topic: this.topicName,
              original_event: event,
              error_message: error.message,
              timestamp: new Date().toISOString(),
            }),
          },
        ],
      });

      logger.info({
        event: 'dlq_message_sent',
        event_id: event?.event_id,
      });
    } catch (dlqError) {
      logger.error({
        event: 'dlq_send_error',
        event_id: event?.event_id,
        error: dlqError instanceof Error ? dlqError.message : 'Unknown error',
      });
      // Re-throw so the source offset is NOT committed and the event is not
      // silently lost when the DLQ is unavailable; Kafka will redeliver it.
      throw dlqError;
    }
  }

  async stop() {
    this.status = 'down';
    await this.consumer.disconnect();
    await this.producer.disconnect();
    logger.info({
      event: 'consumer_stopped',
      topic: this.topicName,
      group: this.groupId,
    });
  }
}
