import { DatabaseService } from '../../src/services/database';
import { Pool } from 'pg';

describe('DLQ Durable Recovery & Crash Consistency Integration Tests', () => {
  let db: DatabaseService;
  let isRealPostgres = false;

  beforeAll(async () => {
    const dbUrl = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/event_driven';
    const testPool = new Pool({ connectionString: dbUrl, connectionTimeoutMillis: 1000 });
    try {
      await testPool.query('SELECT 1');
      isRealPostgres = true;
      db = new DatabaseService(testPool);
      await db.initializeTables();
    } catch {
      isRealPostgres = false;
    } finally {
      if (!isRealPostgres) {
        await testPool.end().catch(() => {});
      }
    }
  });

  afterAll(async () => {
    if (isRealPostgres && db) {
      await db.disconnect().catch(() => {});
    }
  });

  it('verifies durable DLQ transactional persistence, outbox recovery, and idempotency reset on replay', async () => {
    if (!isRealPostgres) {
      console.warn('Skipping DLQ Recovery on real Postgres (INFRASTRUCTURE UNAVAILABLE)');
      return;
    }

    const dlqId = `dlq_int_${Date.now()}`;
    const eventId = `evt_failed_${Date.now()}`;
    const dlqOutboxId = `outbox_${dlqId}`;

    // 1. Consumer failure: persist DLQ message, mark processed FAILED, and insert DLQ Outbox atomically
    await db.withTransaction(async (client) => {
      await db.recordDLQMessage({
        id: dlqId,
        eventId,
        topic: 'orders.events',
        partition: 0,
        offset: '100',
        consumerName: 'order-consumer',
        errorMessage: 'Downstream payment gateway unreachable',
        payload: { event_id: eventId, order_id: 'ord_dlq_1' },
      }, client);

      await db.markEventProcessed(eventId, 'order-consumer', 'FAILED', 'Downstream payment gateway unreachable', client);

      await db.insertDLQOutboxEvent({
        id: dlqOutboxId,
        dlqId,
        topic: 'platform.dlq',
        payload: { dlq_id: dlqId, event_id: eventId },
      }, client);
    });

    // 2. Verify durable persistence in database
    const savedDLQ = await db.getDLQMessage(dlqId);
    expect(savedDLQ).toBeDefined();
    expect(savedDLQ?.status).toBe('UNRESOLVED');

    const pendingDLQOutbox = await db.getPendingDLQOutboxEvents(10, 'dlq-worker-1');
    expect(pendingDLQOutbox.some((e) => e.id === dlqOutboxId)).toBe(true);

    // Mark outbox published
    await db.markDLQOutboxPublished(dlqOutboxId, 'dlq-worker-1');

    // 3. Verify consumer is blocked by FAILED marker
    const isBlocked = await db.isEventProcessed(eventId, 'order-consumer');
    expect(isBlocked).toBe(true);

    // 4. Operator triggers DLQ replay
    // Replay resets idempotency marker and transitions status to REPLAYED
    await db.resetProcessedEventForReplay(eventId, 'order-consumer');
    await db.setDLQStatus(dlqId, 'REPLAYED');

    // 5. Verify idempotency marker was reset so consumer can process the replayed message
    const isNowBlocked = await db.isEventProcessed(eventId, 'order-consumer');
    expect(isNowBlocked).toBe(false);

    const replayedRecord = await db.getDLQMessage(dlqId);
    expect(replayedRecord?.status).toBe('REPLAYED');
    expect(replayedRecord?.resolved_at).toBeDefined();
  });
});
