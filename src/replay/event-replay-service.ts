import { Kafka, Producer } from 'kafkajs';
import { kafkaConfig, producerConfig } from '../config';
import { DatabaseService } from '../services/database';
import { logger } from '../utils/logger';
import { ProjectionConsumer } from '../application/projections/projection-consumer';

export interface ReplayResult {
  replay_id: string;
  target: string;
  events_processed: number;
  duration_ms: number;
  status: 'SUCCESS' | 'FAILED';
  error?: string;
}

export class EventReplayService {
  private kafka: Kafka;
  private producer: Producer;

  constructor(
    private db: DatabaseService,
    private projectionConsumer?: ProjectionConsumer
  ) {
    this.kafka = new Kafka(kafkaConfig);
    this.producer = this.kafka.producer(producerConfig);
  }

  async connect(): Promise<void> {
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }

  /**
   * Replays events from the event store for a specific aggregate or across all aggregates.
   */
  async replayAggregate(aggregateId: string): Promise<ReplayResult> {
    const startTime = Date.now();
    const replayId = `rpl_${Date.now()}`;

    try {
      const events = await this.db.getEventsByAggregateId(aggregateId);
      logger.info({ event: 'replay_started', aggregate_id: aggregateId, total_events: events.length });

      if (this.projectionConsumer) {
        for (const env of events) {
          // Force re-process bypassing idempotency for projection rebuild
          await (this.projectionConsumer as any).processEvent(env);
        }
      }

      const duration = Date.now() - startTime;
      return {
        replay_id: replayId,
        target: aggregateId,
        events_processed: events.length,
        duration_ms: duration,
        status: 'SUCCESS',
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      logger.error({ event: 'replay_error', aggregate_id: aggregateId, error: errorMsg });
      return {
        replay_id: replayId,
        target: aggregateId,
        events_processed: 0,
        duration_ms: duration,
        status: 'FAILED',
        error: errorMsg,
      };
    }
  }

  /**
   * Replays a specific Dead Letter Queue message back to its original target topic.
   */
  async replayDLQ(dlqId: string): Promise<ReplayResult> {
    const startTime = Date.now();
    const dlqMessage = await this.db.getDLQMessage(dlqId);

    if (!dlqMessage) {
      throw new Error(`DLQ message ${dlqId} not found`);
    }

    try {
      const originalEvent = dlqMessage.payload.original_event as Record<string, unknown>;
      const targetTopic = dlqMessage.topic;

      // Clear any prior FAILED record from processed_events so consumer will process the replayed event
      await this.db.resetProcessedEventForReplay(dlqMessage.event_id, dlqMessage.consumer_name);

      // Publish original event back to target topic
      await this.producer.send({
        topic: targetTopic,
        messages: [
          {
            key: (originalEvent.aggregate_id as string) || dlqMessage.event_id,
            value: JSON.stringify(originalEvent),
            headers: {
              'replayed-from-dlq': dlqId,
              'replayed-at': new Date().toISOString(),
            },
          },
        ],
      });

      // Mark DLQ message as REPLAYED
      await this.db.markDLQResolved(dlqId, 'REPLAYED');

      const duration = Date.now() - startTime;
      logger.info({ event: 'dlq_replayed_successfully', dlq_id: dlqId, topic: targetTopic });

      return {
        replay_id: dlqId,
        target: targetTopic,
        events_processed: 1,
        duration_ms: duration,
        status: 'SUCCESS',
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      return {
        replay_id: dlqId,
        target: dlqMessage.topic,
        events_processed: 0,
        duration_ms: duration,
        status: 'FAILED',
        error: errorMsg,
      };
    }
  }
}
