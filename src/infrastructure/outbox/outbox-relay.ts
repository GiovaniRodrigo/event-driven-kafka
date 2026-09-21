import { Kafka, Producer } from 'kafkajs';
import { kafkaConfig, producerConfig } from '../../config';
import { DatabaseService, OutboxEventRow } from '../../services/database';
import { logger } from '../../utils/logger';
import { EventEnvelope } from '../../contracts/envelope';

export interface OutboxRelayOptions {
  batchSize?: number;
  pollIntervalMs?: number;
}

export class OutboxRelay {
  private kafka: Kafka;
  private producer: Producer;
  private isRunning = false;
  private pollTimer?: NodeJS.Timeout;
  private isProcessing = false;
  private batchSize: number;
  private pollIntervalMs: number;

  constructor(
    private db: DatabaseService,
    options: OutboxRelayOptions = {}
  ) {
    this.batchSize = options.batchSize || 50;
    this.pollIntervalMs = options.pollIntervalMs || 150;
    this.kafka = new Kafka(kafkaConfig);
    this.producer = this.kafka.producer(producerConfig);
  }

  async start(): Promise<void> {
    if (this.isRunning) return;

    await this.producer.connect();
    this.isRunning = true;
    logger.info({ event: 'outbox_relay_started', pollIntervalMs: this.pollIntervalMs, batchSize: this.batchSize });

    this.schedulePoll();
  }

  async stop(): Promise<void> {
    this.isRunning = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
    }
    await this.producer.disconnect();
    logger.info({ event: 'outbox_relay_stopped' });
  }

  /**
   * Immediately triggers an outbox sweep (e.g. after a new order is inserted).
   */
  async trigger(): Promise<void> {
    if (!this.isRunning || this.isProcessing) return;
    await this.processBatch();
  }

  private schedulePoll(): void {
    if (!this.isRunning) return;

    this.pollTimer = setTimeout(async () => {
      try {
        await this.processBatch();
      } catch (error) {
        logger.error({
          event: 'outbox_poll_error',
          error: error instanceof Error ? error.message : 'Unknown error',
        });
      } finally {
        this.schedulePoll();
      }
    }, this.pollIntervalMs);
  }

  async processBatch(): Promise<number> {
    if (this.isProcessing) return 0;
    this.isProcessing = true;

    let processedCount = 0;
    try {
      const pendingEvents = await this.db.getPendingOutboxEvents(this.batchSize);
      if (pendingEvents.length === 0) {
        this.isProcessing = false;
        return 0;
      }

      for (const event of pendingEvents) {
        await this.publishEvent(event);
        processedCount++;
      }
    } finally {
      this.isProcessing = false;
    }

    return processedCount;
  }

  private async publishEvent(event: OutboxEventRow): Promise<void> {
    const startTime = Date.now();
    try {
      const envelope: EventEnvelope = {
        event_id: event.id,
        event_type: event.event_type,
        event_version: event.event_version,
        aggregate_id: event.aggregate_id,
        aggregate_type: event.aggregate_type,
        occurred_at: event.created_at.toISOString(),
        producer: 'outbox-relay',
        correlation_id: event.correlation_id,
        causation_id: event.causation_id,
        schema_version: 1,
        payload: event.payload,
      };

      const result = await this.producer.send({
        topic: event.topic,
        messages: [
          {
            key: event.aggregate_id,
            value: JSON.stringify(envelope),
            headers: {
              'correlation-id': event.correlation_id,
              'causation-id': event.causation_id,
              'event-id': event.id,
              'event-type': event.event_type,
              'source-service': 'outbox-relay',
              timestamp: new Date().toISOString(),
            },
          },
        ],
      });

      await this.db.markOutboxEventPublished(event.id);

      const duration = Date.now() - startTime;
      logger.info({
        event: 'outbox_event_published',
        event_id: event.id,
        aggregate_id: event.aggregate_id,
        topic: event.topic,
        event_type: event.event_type,
        partition: result[0]?.partition,
        offset: result[0]?.offset,
        duration_ms: duration,
      });
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      logger.error({
        event: 'outbox_publish_error',
        event_id: event.id,
        aggregate_id: event.aggregate_id,
        topic: event.topic,
        error: errorMsg,
      });

      await this.db.markOutboxEventFailed(event.id, errorMsg);
    }
  }
}
