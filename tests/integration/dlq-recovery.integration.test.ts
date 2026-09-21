import { DatabaseService } from '../../src/services/database';
import { Pool } from 'pg';

describe('DLQ Durable Recovery & Crash Consistency Integration Tests (PostgreSQL)', () => {
  let db: DatabaseService;
  let pool: Pool;

  beforeAll(async () => {
    const dbUrl = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/event_driven_test';
    pool = new Pool({ connectionString: dbUrl, connectionTimeoutMillis: 3000 });
    // This MUST fail loudly if PostgreSQL is unreachable
    await pool.query('SELECT 1');
    db = new DatabaseService(pool);
    await db.initializeTables();
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE dlq_messages, dlq_outbox, processed_events RESTART IDENTITY CASCADE');
  });

  afterAll(async () => {
    if (db) {
      await db.disconnect().catch(() => {});
    }
  });

  it('verifies durable DLQ transactional persistence, outbox recovery after simulated crash, and idempotency reset on replay', async () => {
    const dlqId = `dlq_int_${Date.now()}`;
    const eventId = `evt_failed_${Date.now()}`;
    const dlqOutboxId = `outbox_${dlqId}`;

    // 1. Consumer failure: persist DLQ message, mark processed FAILED, and insert DLQ Outbox atomically in single TX
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

    // 2. Simulate process crash before Kafka network emit.
    // Verify durable persistence in database survives crash:
    const savedDLQ = await db.getDLQMessage(dlqId);
    expect(savedDLQ).toBeDefined();
    expect(savedDLQ?.status).toBe('UNRESOLVED');
    expect(savedDLQ?.error_message).toBe('Downstream payment gateway unreachable');

    // 3. Background relay / recovery worker polls dlq_outbox
    const pendingDLQOutbox = await db.getPendingDLQOutboxEvents(10, 'dlq-worker-1');
    expect(pendingDLQOutbox.some((e) => e.id === dlqOutboxId)).toBe(true);

    // Mark dlq_outbox published
    await db.markDLQOutboxPublished(dlqOutboxId, 'dlq-worker-1');

    // 4. Verify consumer failure marker is persisted in processed_events with FAILED status
    const statusRes = await pool.query('SELECT status FROM processed_events WHERE event_id = $1 AND consumer_name = $2', [eventId, 'order-consumer']);
    expect(statusRes.rows.length).toBe(1);
    expect(statusRes.rows[0].status).toBe('FAILED');

    // 5. Operator triggers DLQ replay: resets idempotency marker and transitions status to REPLAYED
    await db.resetProcessedEventForReplay(eventId, 'order-consumer');
    await db.setDLQStatus(dlqId, 'REPLAYED');

    // 6. Verify idempotency marker was deleted so consumer can process the replayed message
    const resetRes = await pool.query('SELECT status FROM processed_events WHERE event_id = $1 AND consumer_name = $2', [eventId, 'order-consumer']);
    expect(resetRes.rows.length).toBe(0);

    const replayedRecord = await db.getDLQMessage(dlqId);
    expect(replayedRecord?.status).toBe('REPLAYED');
    expect(replayedRecord?.resolved_at).toBeDefined();
  });

  it('guarantees atomic rollback when any DLQ transaction step fails', async () => {
    const dlqId = `dlq_rollback_${Date.now()}`;
    const eventId = `evt_rollback_${Date.now()}`;

    // Execute with intentional failure at end of transaction
    await expect(
      db.withTransaction(async (client) => {
        await db.recordDLQMessage({
          id: dlqId,
          eventId,
          topic: 'orders.events',
          partition: 0,
          offset: '101',
          consumerName: 'order-consumer',
          errorMessage: 'Fatal network glitch',
          payload: { event_id: eventId },
        }, client);

        await db.markEventProcessed(eventId, 'order-consumer', 'FAILED', 'Fatal network glitch', client);

        throw new Error('Simulated crash mid-transaction');
      })
    ).rejects.toThrow('Simulated crash mid-transaction');

    // Verify nothing was committed (Atomic rollback)
    const savedDLQ = await db.getDLQMessage(dlqId);
    expect(savedDLQ).toBeNull();

    const checkRes = await pool.query('SELECT 1 FROM processed_events WHERE event_id = $1', [eventId]);
    expect(checkRes.rows.length).toBe(0);
  });
});
