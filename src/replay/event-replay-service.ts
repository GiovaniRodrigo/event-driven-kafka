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
  reconstructed_state?: any;
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
   * Deterministically reconstructs projection read models for an aggregate from the immutable event store.
   * Cleans/resets the aggregate read model, orders events by sequence number, and replays without external side-effects.
   */
  async replayAggregate(aggregateId: string): Promise<ReplayResult> {
    const startTime = Date.now();
    const replayId = `rpl_${Date.now()}`;

    try {
      logger.info({ event: 'replay.started', aggregate_id: aggregateId, replay_id: replayId });

      // 1. Fetch events ordered strictly by sequence_number
      const events = await this.db.getEventsByAggregateId(aggregateId);

      // 2. Clean/reset read model for this aggregate to guarantee deterministic rebuild from clean state
      await this.db.resetReadModelForAggregate(aggregateId);

      // 3. Apply historical events strictly in order through the projection handler (no side effects)
      if (this.projectionConsumer) {
        for (const env of events) {
          await this.projectionConsumer.applyHistoricalEvent(env);
        }
      }

      // 4. Capture reconstructed read model state
      const reconstructedState = await this.db.getOrder(aggregateId);

      const duration = Date.now() - startTime;
      logger.info({
        event: 'replay.completed',
        aggregate_id: aggregateId,
        replay_id: replayId,
        events_processed: events.length,
        duration_ms: duration,
      });

      return {
        replay_id: replayId,
        target: aggregateId,
        events_processed: events.length,
        duration_ms: duration,
        reconstructed_state: reconstructedState,
        status: 'SUCCESS',
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      logger.error({ event: 'replay.failed', aggregate_id: aggregateId, replay_id: replayId, error: errorMsg });
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
   * Replays a specific Dead Letter Queue message back to its original target topic safely.
   */
  async replayDLQ(dlqId: string): Promise<ReplayResult> {
    const startTime = Date.now();
    const dlqMessage = await this.db.getDLQMessage(dlqId);

    if (!dlqMessage) {
      throw new Error(`DLQ message ${dlqId} not found`);
    }

    if (dlqMessage.status === 'DISCARDED') {
      throw new Error(`Cannot replay discarded DLQ message ${dlqId}`);
    }

    try {
      logger.info({ event: 'dlq.replay_started', dlq_id: dlqId, topic: dlqMessage.topic });

      // 1. Mark status as REPLAYING
      await this.db.setDLQStatus(dlqId, 'REPLAYING');

      const originalEvent = dlqMessage.payload.original_event as Record<string, unknown>;
      const targetTopic = dlqMessage.topic;

      // 2. Clear any prior FAILED record from processed_events so consumer will process the replayed event
      await this.db.resetProcessedEventForReplay(dlqMessage.event_id, dlqMessage.consumer_name);

      // 3. Publish original event back to target topic
      await this.producer.send({
        topic: targetTopic,
        messages: [
          {
            key: (originalEvent.aggregate_id as string) || dlqMessage.event_id,
            value: JSON.stringify(originalEvent),
            headers: {
              'replayed-from-dlq': dlqId,
              'replayed-at': new Date().toISOString(),
              'source-dlq-id': dlqId,
            },
          },
        ],
      });

      // 4. Mark DLQ message as REPLAYED only after Kafka send succeeds
      await this.db.markDLQResolved(dlqId, 'REPLAYED');

      const duration = Date.now() - startTime;
      logger.info({ event: 'dlq.replay_completed', dlq_id: dlqId, topic: targetTopic, duration_ms: duration });

      return {
        replay_id: dlqId,
        target: targetTopic,
        events_processed: 1,
        duration_ms: duration,
        status: 'SUCCESS',
      };
    } catch (error) {
      // Revert status to UNRESOLVED so it remains recoverable
      try {
        await this.db.setDLQStatus(dlqId, 'UNRESOLVED');
      } catch {}

      const duration = Date.now() - startTime;
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      logger.error({ event: 'dlq.replay_failed', dlq_id: dlqId, error: errorMsg });
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
