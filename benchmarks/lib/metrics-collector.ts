import { DatabaseService } from '../../src/services/database';
import { Kafka } from 'kafkajs';
import { kafkaConfig, topics } from '../../src/config';

export interface LatencyStats {
  count: number;
  p50: number;
  p95: number;
  p99: number;
  min: number;
  max: number;
  avg: number;
}

export interface StepLatencyStats {
  step: string;
  count: number;
  p50: number;
  p95: number;
  p99: number;
  avg: number;
}

export interface E2ELatencyReport {
  fulfillmentLatency: LatencyStats;
  stepLatencies: StepLatencyStats[];
  totalOrdersSampled: number;
  totalCompletedOrders: number;
}

export interface ConsumerLagInfo {
  consumerGroup: string;
  topic: string;
  partition: number;
  currentOffset: number;
  logEndOffset: number;
  lag: number;
}

export interface ConsumerGroupLagSummary {
  consumerGroup: string;
  totalLag: number;
  partitions: ConsumerLagInfo[];
}

export interface ClusterLagReport {
  groups: ConsumerGroupLagSummary[];
  totalClusterLag: number;
  timestamp: string;
}

export interface OutboxDrainMetrics {
  initialPending: number;
  peakPending: number;
  finalPending: number;
  publishedCount: number;
  failedCount: number;
  drainTimeSeconds: number;
  drainRatePerSec: number;
}

export class MetricsCollector {
  private kafka: Kafka;
  private db: DatabaseService;

  constructor(db?: DatabaseService) {
    this.db = db || new DatabaseService();
    this.kafka = new Kafka(kafkaConfig);
  }

  async getDatabaseMetrics() {
    return this.db.getMetrics();
  }

  async getKafkaConsumerLag(): Promise<ClusterLagReport> {
    const admin = this.kafka.admin();
    const consumerGroups = [
      { groupId: 'payment-service-group', topics: [topics.payments.name] },
      { groupId: 'inventory-service-group', topics: [topics.inventory.name] },
      { groupId: 'fraud-service-group', topics: [topics.fraud.name] },
      { groupId: 'shipping-service-group', topics: [topics.shipping.name] },
      { groupId: 'notification-service-group', topics: [topics.notifications.name] },
      {
        groupId: 'saga-orchestrator-group',
        topics: [
          topics.orders.name,
          topics.payments.name,
          topics.inventory.name,
          topics.fraud.name,
          topics.shipping.name,
        ],
      },
      {
        groupId: 'projection-read-model-group',
        topics: [
          topics.orders.name,
          topics.payments.name,
          topics.inventory.name,
          topics.fraud.name,
          topics.shipping.name,
          topics.notifications.name,
        ],
      },
    ];

    const groupSummaries: ConsumerGroupLagSummary[] = [];
    let totalClusterLag = 0;

    try {
      await admin.connect();

      // Fetch topic end offsets for all relevant topics
      const allTopicNames = Object.values(topics).map((t) => t.name);
      const topicOffsetsMap = new Map<string, Map<number, number>>();

      for (const topic of allTopicNames) {
        try {
          const topicOffsets = await admin.fetchTopicOffsets(topic);
          const partMap = new Map<number, number>();
          for (const to of topicOffsets) {
            partMap.set(to.partition, parseInt(to.high, 10));
          }
          topicOffsetsMap.set(topic, partMap);
        } catch {
          // Topic might not exist yet
        }
      }

      for (const cg of consumerGroups) {
        let groupTotalLag = 0;
        const partitionInfos: ConsumerLagInfo[] = [];

        try {
          const groupOffsets = await admin.fetchOffsets({
            groupId: cg.groupId,
            topics: cg.topics,
          });

          for (const topicOffset of groupOffsets) {
            const topicName = topicOffset.topic;
            const highOffsets = topicOffsetsMap.get(topicName) || new Map<number, number>();

            for (const part of topicOffset.partitions) {
              const currentOffset = parseInt(part.offset, 10);
              const logEndOffset = highOffsets.get(part.partition) ?? 0;
              // If consumer hasn't committed yet (-1), consumer starts at log end offset
              const effectiveOffset = currentOffset >= 0 ? currentOffset : logEndOffset;
              const lag = Math.max(0, logEndOffset - effectiveOffset);

              groupTotalLag += lag;
              partitionInfos.push({
                consumerGroup: cg.groupId,
                topic: topicName,
                partition: part.partition,
                currentOffset: effectiveOffset,
                logEndOffset,
                lag,
              });
            }
          }
        } catch {
          // Consumer group may not have active members or committed offsets yet
        }

        groupSummaries.push({
          consumerGroup: cg.groupId,
          totalLag: groupTotalLag,
          partitions: partitionInfos,
        });

        totalClusterLag += groupTotalLag;
      }
    } finally {
      await admin.disconnect();
    }

    return {
      groups: groupSummaries,
      totalClusterLag,
      timestamp: new Date().toISOString(),
    };
  }

  async calculateE2ELatencies(_sinceTimestamp?: Date): Promise<E2ELatencyReport> {
    const pool = this.db.getPool();

    // Query event store using created_at for accurate DB-level timing
    const query = `
      SELECT aggregate_id, event_type, created_at
      FROM event_store
      ORDER BY aggregate_id, sequence_number ASC, created_at ASC
    `;

    const result = await pool.query(query);
    const orderEventsMap = new Map<string, { [eventType: string]: Date }>();

    for (const row of result.rows) {
      const orderId = row.aggregate_id;
      const eventType = row.event_type;
      const createdAt = new Date(row.created_at);

      if (!orderEventsMap.has(orderId)) {
        orderEventsMap.set(orderId, {});
      }
      const events = orderEventsMap.get(orderId)!;
      if (!events[eventType]) {
        events[eventType] = createdAt;
      }
    }

    // Also merge from order_events for comprehensive coverage
    const orderEventsRes = await pool.query(
      'SELECT order_id, event_type, created_at FROM order_events ORDER BY order_id, id ASC'
    );
    for (const row of orderEventsRes.rows) {
      const orderId = row.order_id;
      const eventType = row.event_type;
      const createdAt = new Date(row.created_at);

      if (!orderEventsMap.has(orderId)) {
        orderEventsMap.set(orderId, {});
      }
      const events = orderEventsMap.get(orderId)!;
      if (!events[eventType]) {
        events[eventType] = createdAt;
      }
    }

    const fulfillmentDurations: number[] = [];
    const stepDurations: { [step: string]: number[] } = {
      'OrderCreated -> PaymentAuthorized': [],
      'PaymentAuthorized -> InventoryReserved': [],
      'InventoryReserved -> FraudApproved': [],
      'FraudApproved -> ShipmentCreated': [],
      'ShipmentCreated -> OrderCompleted': [],
    };

    let completedOrders = 0;

    for (const [_orderId, events] of orderEventsMap.entries()) {
      const created = events['OrderCreated'];
      const completed = events['OrderCompleted'];

      if (created && completed) {
        const totalDuration = completed.getTime() - created.getTime();
        if (totalDuration >= 0) {
          fulfillmentDurations.push(totalDuration);
          completedOrders++;
        }
      }

      // Calculate step-to-step latencies
      if (events['OrderCreated'] && events['PaymentAuthorized']) {
        const d = events['PaymentAuthorized'].getTime() - events['OrderCreated'].getTime();
        if (d >= 0) stepDurations['OrderCreated -> PaymentAuthorized'].push(d);
      }
      if (events['PaymentAuthorized'] && events['InventoryReserved']) {
        const d = events['InventoryReserved'].getTime() - events['PaymentAuthorized'].getTime();
        if (d >= 0) stepDurations['PaymentAuthorized -> InventoryReserved'].push(d);
      }
      if (events['InventoryReserved'] && events['FraudApproved']) {
        const d = events['FraudApproved'].getTime() - events['InventoryReserved'].getTime();
        if (d >= 0) stepDurations['InventoryReserved -> FraudApproved'].push(d);
      }
      if (events['FraudApproved'] && events['ShipmentCreated']) {
        const d = events['ShipmentCreated'].getTime() - events['FraudApproved'].getTime();
        if (d >= 0) stepDurations['FraudApproved -> ShipmentCreated'].push(d);
      }
      if (events['ShipmentCreated'] && events['OrderCompleted']) {
        const d = events['OrderCompleted'].getTime() - events['ShipmentCreated'].getTime();
        if (d >= 0) stepDurations['ShipmentCreated -> OrderCompleted'].push(d);
      }
    }

    const fulfillmentStats = this.computeStats(fulfillmentDurations);
    const stepStatsList: StepLatencyStats[] = Object.entries(stepDurations).map(([step, durations]) => {
      const s = this.computeStats(durations);
      return {
        step,
        count: s.count,
        p50: s.p50,
        p95: s.p95,
        p99: s.p99,
        avg: s.avg,
      };
    });

    return {
      fulfillmentLatency: fulfillmentStats,
      stepLatencies: stepStatsList,
      totalOrdersSampled: orderEventsMap.size,
      totalCompletedOrders: completedOrders,
    };
  }

  async calculateSagaLatencies(_sinceTimestamp?: Date) {
    const pool = this.db.getPool();

    const query = `
      SELECT saga_id, state, created_at, updated_at
      FROM saga_instances
    `;

    const result = await pool.query(query);

    const completedDurations: number[] = [];
    const compensatingDurations: number[] = [];
    let completedCount = 0;
    let compensatedCount = 0;
    let failedCount = 0;

    for (const row of result.rows) {
      const created = new Date(row.created_at).getTime();
      const updated = new Date(row.updated_at).getTime();
      const duration = Math.max(0, updated - created);

      if (row.state === 'COMPLETED') {
        completedDurations.push(duration);
        completedCount++;
      } else if (row.state === 'CANCELLED') {
        compensatingDurations.push(duration);
        compensatedCount++;
      } else if (row.state === 'FAILED') {
        failedCount++;
      }
    }

    return {
      successfulSagas: {
        count: completedCount,
        stats: this.computeStats(completedDurations),
      },
      compensatingSagas: {
        count: compensatedCount,
        stats: this.computeStats(compensatingDurations),
      },
      failedSagas: failedCount,
      totalSagas: result.rows.length,
    };
  }

  computeStats(durations: number[]): LatencyStats {
    if (durations.length === 0) {
      return { count: 0, p50: 0, p95: 0, p99: 0, min: 0, max: 0, avg: 0 };
    }

    const sorted = [...durations].sort((a, b) => a - b);
    const count = sorted.length;
    const p50 = sorted[Math.floor(count * 0.5)];
    const p95 = sorted[Math.floor(count * 0.95)];
    const p99 = sorted[Math.floor(count * 0.99)];
    const min = sorted[0];
    const max = sorted[count - 1];
    const sum = sorted.reduce((acc, v) => acc + v, 0);
    const avg = parseFloat((sum / count).toFixed(2));

    return { count, p50, p95, p99, min, max, avg };
  }

  async waitForOutboxDrain(maxWaitSeconds = 30): Promise<{ drained: boolean; pending: number; timeSec: number }> {
    const pool = this.db.getPool();
    const start = Date.now();

    while ((Date.now() - start) / 1000 < maxWaitSeconds) {
      const res = await pool.query(
        "SELECT COUNT(*) as pending FROM outbox_events WHERE status IN ('PENDING', 'PROCESSING')"
      );
      const pending = parseInt(res.rows[0].pending, 10);
      if (pending === 0) {
        return { drained: true, pending: 0, timeSec: (Date.now() - start) / 1000 };
      }
      await new Promise((r) => setTimeout(r, 200));
    }

    const res = await pool.query(
      "SELECT COUNT(*) as pending FROM outbox_events WHERE status IN ('PENDING', 'PROCESSING')"
    );
    const pending = parseInt(res.rows[0].pending, 10);
    return { drained: pending === 0, pending, timeSec: (Date.now() - start) / 1000 };
  }

  async waitForConsumerLagZero(maxWaitSeconds = 30): Promise<{ zeroLag: boolean; totalLag: number; timeSec: number }> {
    const start = Date.now();

    while ((Date.now() - start) / 1000 < maxWaitSeconds) {
      const lagReport = await this.getKafkaConsumerLag();
      if (lagReport.totalClusterLag === 0) {
        return { zeroLag: true, totalLag: 0, timeSec: (Date.now() - start) / 1000 };
      }
      await new Promise((r) => setTimeout(r, 300));
    }

    const finalLag = await this.getKafkaConsumerLag();
    return {
      zeroLag: finalLag.totalClusterLag === 0,
      totalLag: finalLag.totalClusterLag,
      timeSec: (Date.now() - start) / 1000,
    };
  }

  async waitForAllOrdersCompleted(expectedCount: number, maxWaitSeconds = 120): Promise<{ allCompleted: boolean; count: number; timeSec: number }> {
    const pool = this.db.getPool();
    const start = Date.now();

    while ((Date.now() - start) / 1000 < maxWaitSeconds) {
      try {
        const res = await pool.query(
          "SELECT COUNT(*) as completed FROM saga_instances WHERE state IN ('COMPLETED', 'CANCELLED', 'FAILED')"
        );
        const count = parseInt(res.rows[0].completed, 10);
        if (count >= expectedCount) {
          // Wait 1.5s for projection-consumer to write final read-model status
          await new Promise((r) => setTimeout(r, 1500));
          return { allCompleted: true, count, timeSec: (Date.now() - start) / 1000 };
        }
      } catch {
        // Ignored if DB is temporarily reconnecting
      }
      await new Promise((r) => setTimeout(r, 500));
    }

    try {
      const res = await pool.query(
        "SELECT COUNT(*) as completed FROM saga_instances WHERE state IN ('COMPLETED', 'CANCELLED', 'FAILED')"
      );
      const count = parseInt(res.rows[0].completed, 10);
      return { allCompleted: count >= expectedCount, count, timeSec: (Date.now() - start) / 1000 };
    } catch {
      return { allCompleted: false, count: 0, timeSec: (Date.now() - start) / 1000 };
    }
  }
}
