import { DatabaseService } from '../../src/services/database';

describe('Outbox Lease Fencing & Stale Worker Protection Unit Tests', () => {
  let dbMock: any;
  let outboxTable: Map<string, any>;

  beforeEach(() => {
    outboxTable = new Map();

    dbMock = {
      pool: {
        query: jest.fn().mockImplementation(async (sql: string, params: any[] = []) => {
          if (sql.includes('UPDATE outbox_events') && sql.includes('SET status = \'PUBLISHED\'')) {
            const id = params[0];
            const workerId = params[1];
            const row = outboxTable.get(id);

            // Enforce lease fencing: row must be PROCESSING and lease_owner must match
            if (row && row.status === 'PROCESSING' && (!workerId || row.lease_owner === workerId)) {
              row.status = 'PUBLISHED';
              row.published_at = new Date();
              row.lease_owner = null;
              row.lease_expires_at = null;
              outboxTable.set(id, row);
              return { rowCount: 1, rows: [row] };
            }
            return { rowCount: 0, rows: [] };
          }

          if (sql.includes('UPDATE outbox_events') && sql.includes('SET last_error = $2')) {
            const id = params[0];
            const error = params[1];
            const workerId = params[2];
            const row = outboxTable.get(id);

            if (row && row.status === 'PROCESSING' && (!workerId || row.lease_owner === workerId)) {
              row.last_error = error;
              row.attempts = (row.attempts || 0) + 1;
              row.status = row.attempts >= 10 ? 'FAILED' : 'PENDING';
              row.lease_owner = null;
              row.lease_expires_at = null;
              outboxTable.set(id, row);
              return { rowCount: 1, rows: [row] };
            }
            return { rowCount: 0, rows: [] };
          }

          return { rowCount: 0, rows: [] };
        }),
      },
      markOutboxEventPublished: DatabaseService.prototype.markOutboxEventPublished,
      markOutboxEventFailed: DatabaseService.prototype.markOutboxEventFailed,
    };
  });

  it('rejects stale worker completion when lease has been reclaimed by another worker', async () => {
    const eventId = 'evt_fence_1';

    // 1. Worker A initially acquired event
    outboxTable.set(eventId, {
      id: eventId,
      aggregate_id: 'ord_fence_1',
      topic: 'orders.events',
      status: 'PROCESSING',
      lease_owner: 'worker-A',
      lease_expires_at: new Date(Date.now() - 5000), // Expired 5 seconds ago
      attempts: 1,
    });

    // 2. Worker B reclaims the expired event
    outboxTable.set(eventId, {
      ...outboxTable.get(eventId),
      lease_owner: 'worker-B',
      lease_expires_at: new Date(Date.now() + 30000), // Valid for 30s
      attempts: 2,
    });

    // 3. Worker A finishes late and tries to mark as PUBLISHED
    const workerAResult = await dbMock.markOutboxEventPublished(eventId, 'worker-A');
    expect(workerAResult).toBe(false);

    // Verify row is STILL owned by Worker B and in PROCESSING state
    let row = outboxTable.get(eventId);
    expect(row.status).toBe('PROCESSING');
    expect(row.lease_owner).toBe('worker-B');

    // 4. Worker A tries to mark as FAILED
    const workerAFailResult = await dbMock.markOutboxEventFailed(eventId, 'Timeout error', 'worker-A');
    expect(workerAFailResult).toBe(false);

    // Verify row is STILL owned by Worker B
    row = outboxTable.get(eventId);
    expect(row.status).toBe('PROCESSING');
    expect(row.lease_owner).toBe('worker-B');

    // 5. Worker B finishes successfully and marks PUBLISHED
    const workerBResult = await dbMock.markOutboxEventPublished(eventId, 'worker-B');
    expect(workerBResult).toBe(true);

    // Verify row is now PUBLISHED and leases cleared
    row = outboxTable.get(eventId);
    expect(row.status).toBe('PUBLISHED');
    expect(row.lease_owner).toBeNull();
  });

  it('allows completion when worker still holds authoritative active lease', async () => {
    const eventId = 'evt_fence_2';

    outboxTable.set(eventId, {
      id: eventId,
      aggregate_id: 'ord_fence_2',
      topic: 'orders.events',
      status: 'PROCESSING',
      lease_owner: 'worker-authoritative',
      lease_expires_at: new Date(Date.now() + 25000),
      attempts: 1,
    });

    const result = await dbMock.markOutboxEventPublished(eventId, 'worker-authoritative');
    expect(result).toBe(true);

    const row = outboxTable.get(eventId);
    expect(row.status).toBe('PUBLISHED');
    expect(row.lease_owner).toBeNull();
  });
});
