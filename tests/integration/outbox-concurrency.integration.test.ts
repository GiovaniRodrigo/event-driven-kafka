import { DatabaseService } from '../../src/services/database';
import { Pool } from 'pg';

describe('Outbox Concurrency & Lease Fencing Integration Tests (PostgreSQL)', () => {
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
    await pool.query('TRUNCATE outbox_events RESTART IDENTITY CASCADE');
  });

  afterAll(async () => {
    if (db) {
      await db.disconnect().catch(() => {});
    }
  });

  it('Scenario A: Two concurrent workers claim mutually exclusive pending events', async () => {
    const eventId1 = `evt_conc_${Date.now()}_1`;
    const eventId2 = `evt_conc_${Date.now()}_2`;

    await db.insertOutboxEvent({
      id: eventId1,
      aggregateId: 'ord_conc_1',
      aggregateType: 'Order',
      eventType: 'OrderCreated',
      payload: { test: true },
      correlationId: 'corr_1',
      causationId: 'cmd_1',
      topic: 'orders.events',
    });

    await db.insertOutboxEvent({
      id: eventId2,
      aggregateId: 'ord_conc_2',
      aggregateType: 'Order',
      eventType: 'OrderCreated',
      payload: { test: true },
      correlationId: 'corr_2',
      causationId: 'cmd_2',
      topic: 'orders.events',
    });

    const [batchA, batchB] = await Promise.all([
      db.getPendingOutboxEvents(1, 'worker-A', 30),
      db.getPendingOutboxEvents(1, 'worker-B', 30),
    ]);

    const claimedIdsA = batchA.map((e) => e.id);
    const claimedIdsB = batchB.map((e) => e.id);

    // Mutual exclusion: no event claimed by both workers
    const intersection = claimedIdsA.filter((id) => claimedIdsB.includes(id));
    expect(intersection.length).toBe(0);
    expect(claimedIdsA.length + claimedIdsB.length).toBe(2);
  });

  it('Scenario B: Expired lease is reclaimed by failover worker', async () => {
    const eventId = `evt_expire_${Date.now()}`;
    await db.insertOutboxEvent({
      id: eventId,
      aggregateId: 'ord_exp_1',
      aggregateType: 'Order',
      eventType: 'OrderCreated',
      payload: { test: true },
      correlationId: 'corr_exp',
      causationId: 'cmd_exp',
      topic: 'orders.events',
    });

    // Worker 1 acquires lease for only 1 second
    const batch1 = await db.getPendingOutboxEvents(10, 'worker-crash-1', 1);
    expect(batch1.some((e) => e.id === eventId)).toBe(true);

    // Wait 1.5s for lease to expire
    await new Promise((r) => setTimeout(r, 1500));

    // Worker 2 reclaims expired row
    const batch2 = await db.getPendingOutboxEvents(10, 'worker-failover-2', 30);
    const reclaimed = batch2.find((e) => e.id === eventId);
    expect(reclaimed).toBeDefined();
    expect(reclaimed?.lease_owner).toBe('worker-failover-2');
  });

  it('Scenario C: Stale worker completion is rejected by PostgreSQL lease fencing', async () => {
    const eventId = `evt_stale_${Date.now()}`;
    await db.insertOutboxEvent({
      id: eventId,
      aggregateId: 'ord_stale_1',
      aggregateType: 'Order',
      eventType: 'OrderCreated',
      payload: { test: true },
      correlationId: 'corr_stale',
      causationId: 'cmd_stale',
      topic: 'orders.events',
    });

    // Worker 1 acquires lease for 1s
    await db.getPendingOutboxEvents(10, 'worker-stale-1', 1);

    // Wait for expiration
    await new Promise((r) => setTimeout(r, 1500));

    // Worker 2 reclaims authoritative lease
    await db.getPendingOutboxEvents(10, 'worker-auth-2', 30);

    // Stale Worker 1 attempts completion
    const staleResult = await db.markOutboxEventPublished(eventId, 'worker-stale-1');
    expect(staleResult).toBe(false);

    // Verify row status in PostgreSQL is still PROCESSING with worker-auth-2
    const res = await pool.query('SELECT status, lease_owner, published_at FROM outbox_events WHERE id = $1', [eventId]);
    expect(res.rows[0].status).toBe('PROCESSING');
    expect(res.rows[0].lease_owner).toBe('worker-auth-2');
    expect(res.rows[0].published_at).toBeNull();

    // Authoritative Worker 2 finishes and succeeds
    const authResult = await db.markOutboxEventPublished(eventId, 'worker-auth-2');
    expect(authResult).toBe(true);

    // Verify row status is now PUBLISHED in PostgreSQL
    const resAfter = await pool.query('SELECT status, lease_owner, published_at FROM outbox_events WHERE id = $1', [eventId]);
    expect(resAfter.rows[0].status).toBe('PUBLISHED');
    expect(resAfter.rows[0].published_at).not.toBeNull();
  });

  it('Scenario D: Stale worker failure marking cannot corrupt active worker lease', async () => {
    const eventId = `evt_stale_fail_${Date.now()}`;
    await db.insertOutboxEvent({
      id: eventId,
      aggregateId: 'ord_stale_fail_1',
      aggregateType: 'Order',
      eventType: 'OrderCreated',
      payload: { test: true },
      correlationId: 'corr_stale_fail',
      causationId: 'cmd_stale_fail',
      topic: 'orders.events',
    });

    // Worker 1 acquires lease for 1s
    await db.getPendingOutboxEvents(10, 'worker-stale-f1', 1);

    // Wait for expiration
    await new Promise((r) => setTimeout(r, 1500));

    // Worker 2 reclaims authoritative lease
    await db.getPendingOutboxEvents(10, 'worker-auth-f2', 30);

    // Stale Worker 1 attempts failure marking
    const staleFailResult = await db.markOutboxEventFailed(eventId, 'worker-stale-f1', 'Timeout in stale worker');
    expect(staleFailResult).toBe(false);

    // Verify row status in PostgreSQL remains PROCESSING under worker-auth-f2
    const res = await pool.query('SELECT status, lease_owner FROM outbox_events WHERE id = $1', [eventId]);
    expect(res.rows[0].status).toBe('PROCESSING');
    expect(res.rows[0].lease_owner).toBe('worker-auth-f2');
  });
});
