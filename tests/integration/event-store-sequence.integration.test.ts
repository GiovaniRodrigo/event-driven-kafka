import { DatabaseService } from '../../src/services/database';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';
import { Pool } from 'pg';

describe('Event Store Sequence Integrity & Advisory Locking Integration Tests (PostgreSQL)', () => {
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
    await pool.query('TRUNCATE event_store RESTART IDENTITY CASCADE');
  });

  afterAll(async () => {
    if (db) {
      await db.disconnect().catch(() => {});
    }
  });

  it('guarantees unique, strictly contiguous sequence numbers (1..25) under high concurrent write load', async () => {
    const aggregateId = `ord_seq_high_${Date.now()}`;
    const CONCURRENCY_COUNT = 25;

    // Create 25 distinct event envelopes for the same aggregate ID
    const envelopes = Array.from({ length: CONCURRENCY_COUNT }, (_, i) =>
      createEventEnvelope({
        eventType: i === 0 ? EventTypes.OrderCreated : EventTypes.PaymentAuthorized,
        aggregateId,
        aggregateType: 'Order',
        producer: 'load-test',
        correlationId: `corr_${aggregateId}`,
        causationId: `cmd_${i + 1}`,
        payload: { iteration: i + 1, order_id: aggregateId },
      })
    );

    // Append all 25 concurrently across parallel promises (relies on pg_advisory_xact_lock in appendToEventStore)
    const appendPromises = envelopes.map((env) => db.appendToEventStore(env));
    await Promise.all(appendPromises);

    // Query event store directly from PostgreSQL
    const events = await db.getEventsByAggregateId(aggregateId);
    expect(events.length).toBe(CONCURRENCY_COUNT);

    const sequenceNumbers = events.map((e) => e.sequence_number);

    // 1. Sequence numbers must be strictly unique (no duplicate sequence numbers)
    const uniqueSequences = new Set(sequenceNumbers);
    expect(uniqueSequences.size).toBe(CONCURRENCY_COUNT);

    // 2. Sequence numbers must be strictly contiguous from 1 to 25
    const expectedSequences = Array.from({ length: CONCURRENCY_COUNT }, (_, i) => i + 1);
    expect(sequenceNumbers).toEqual(expectedSequences);
  });

  it('enforces append-only immutability via database trigger (trg_prevent_event_store_mutation)', async () => {
    const aggregateId = `ord_immutable_${Date.now()}`;
    const env = createEventEnvelope({
      eventType: EventTypes.OrderCreated,
      aggregateId,
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId: `corr_${aggregateId}`,
      causationId: 'cmd_init',
      payload: { order_id: aggregateId },
    });

    await db.appendToEventStore(env);

    // Attempt direct SQL UPDATE on event_store - MUST throw trigger violation
    await expect(
      pool.query("UPDATE event_store SET event_type = 'MutatedEvent' WHERE aggregate_id = $1", [aggregateId])
    ).rejects.toThrow();

    // Attempt direct SQL DELETE on event_store - MUST throw trigger violation
    await expect(
      pool.query('DELETE FROM event_store WHERE aggregate_id = $1', [aggregateId])
    ).rejects.toThrow();
  });
});
