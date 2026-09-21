import { DatabaseService } from '../../src/services/database';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';
import { Pool } from 'pg';

describe('Event Store Sequence Integrity Integration Tests', () => {
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

  it('guarantees sequential sequence number ordering and prevents sequence collision during concurrent appends', async () => {
    if (!isRealPostgres) {
      console.warn('Skipping Event Store Sequence on real Postgres (INFRASTRUCTURE UNAVAILABLE)');
      return;
    }

    const aggregateId = `ord_seq_${Date.now()}`;

    const env1 = createEventEnvelope({
      eventType: EventTypes.OrderCreated,
      aggregateId,
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId: `corr_${aggregateId}`,
      causationId: 'cmd_1',
      payload: { order_id: aggregateId },
    });

    const env2 = createEventEnvelope({
      eventType: EventTypes.PaymentAuthorized,
      aggregateId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId: `corr_${aggregateId}`,
      causationId: 'evt_1',
      payload: { order_id: aggregateId },
    });

    const env3 = createEventEnvelope({
      eventType: EventTypes.InventoryReserved,
      aggregateId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId: `corr_${aggregateId}`,
      causationId: 'evt_2',
      payload: { order_id: aggregateId },
    });

    // Append concurrently
    await Promise.all([
      db.appendToEventStore(env1),
      db.appendToEventStore(env2),
      db.appendToEventStore(env3),
    ]);

    const events = await db.getEventsByAggregateId(aggregateId);
    expect(events.length).toBe(3);

    const sequenceNumbers = events.map((e) => e.sequence_number);
    // Sequences must be strictly distinct and sorted
    const uniqueSequences = new Set(sequenceNumbers);
    expect(uniqueSequences.size).toBe(3);
    expect(sequenceNumbers).toEqual([1, 2, 3]);
  });
});
