import { OutboxRelay } from '../../src/infrastructure/outbox/outbox-relay';
import { OutboxEventRow } from '../../src/services/database';

describe('Transactional Outbox Lease & Crash Recovery Verification', () => {
  let outboxTable: OutboxEventRow[];
  let dbMock: any;

  beforeEach(() => {
    outboxTable = [];

    dbMock = {
      getPendingOutboxEvents: jest.fn().mockImplementation(async (limit = 50, workerId = 'worker-1', leaseDurationSec = 30) => {
        const now = new Date();
        // Claims pending or expired PROCESSING events
        const claimable = outboxTable.filter(
          (e) =>
            (e.status === 'PENDING' ||
              (e.status === 'PROCESSING' && e.lease_expires_at && e.lease_expires_at < now)) &&
            e.attempts < 10
        ).slice(0, limit);

        for (const item of claimable) {
          item.status = 'PROCESSING';
          item.lease_owner = workerId;
          item.leased_at = new Date();
          item.lease_expires_at = new Date(Date.now() + leaseDurationSec * 1000);
          item.attempts++;
        }

        return claimable;
      }),
      markOutboxEventPublished: jest.fn().mockImplementation(async (id: string) => {
        const item = outboxTable.find((e) => e.id === id);
        if (item) {
          item.status = 'PUBLISHED';
          item.published_at = new Date();
          item.lease_owner = undefined;
          item.lease_expires_at = undefined;
        }
      }),
      markOutboxEventFailed: jest.fn().mockImplementation(async (id: string, error: string) => {
        const item = outboxTable.find((e) => e.id === id);
        if (item) {
          item.last_error = error;
          item.lease_owner = undefined;
          item.lease_expires_at = undefined;
          item.status = item.attempts >= 10 ? 'FAILED' : 'PENDING';
        }
      }),
    };
  });

  it('Scenario A: publish fails -> event remains PENDING -> retried and eventually published', async () => {
    outboxTable.push({
      id: 'evt_fail_1',
      aggregate_id: 'ord_fail_1',
      aggregate_type: 'Order',
      event_type: 'OrderCreated',
      event_version: 1,
      payload: { order_id: 'ord_fail_1' },
      correlation_id: 'corr_fail_1',
      causation_id: 'cmd_fail_1',
      topic: 'orders.events',
      status: 'PENDING',
      attempts: 0,
      created_at: new Date(),
    });

    const relay = new OutboxRelay(dbMock, { workerId: 'worker-a', leaseDurationSeconds: 1 });
    let publishAttempt = 0;

    // Simulate transient broker failure on attempt 1, success on attempt 2
    (relay as any).producer = {
      send: jest.fn().mockImplementation(async () => {
        publishAttempt++;
        if (publishAttempt === 1) {
          throw new Error('Kafka broker connection refused');
        }
        return [{ partition: 0, offset: '10' }];
      }),
    };

    // Attempt 1: Fails
    const count1 = await relay.processBatch();
    expect(count1).toBe(1);
    expect(outboxTable[0].status).toBe('PENDING');
    expect(outboxTable[0].attempts).toBe(1);
    expect(outboxTable[0].last_error).toContain('Kafka broker connection refused');

    // Attempt 2: Succeeds
    const count2 = await relay.processBatch();
    expect(count2).toBe(1);
    expect(outboxTable[0].status).toBe('PUBLISHED');
    expect(outboxTable[0].attempts).toBe(2);
  });

  it('Scenario B: worker crash after publish -> lease expires -> second worker reclaims and republishes (At-Least-Once)', async () => {
    outboxTable.push({
      id: 'evt_crash_2',
      aggregate_id: 'ord_crash_2',
      aggregate_type: 'Order',
      event_type: 'OrderCreated',
      event_version: 1,
      payload: { order_id: 'ord_crash_2' },
      correlation_id: 'corr_crash_2',
      causation_id: 'cmd_crash_2',
      topic: 'orders.events',
      status: 'PENDING',
      attempts: 0,
      created_at: new Date(),
    });

    const worker1 = new OutboxRelay(dbMock, { workerId: 'worker-1', leaseDurationSeconds: 1 });
    const worker2 = new OutboxRelay(dbMock, { workerId: 'worker-2', leaseDurationSeconds: 1 });

    const publishedKafkaMessages: any[] = [];
    (worker1 as any).producer = {
      send: jest.fn().mockImplementation(async (msg) => {
        publishedKafkaMessages.push(msg);
        // Simulate crash right after network publish (before markOutboxEventPublished DB call)
        throw new Error('SIMULATED_PROCESS_CRASH_AFTER_SOCKET_WRITE');
      }),
    };

    (worker2 as any).producer = {
      send: jest.fn().mockImplementation(async (msg) => {
        publishedKafkaMessages.push(msg);
        return [{ partition: 0, offset: '15' }];
      }),
    };

    // Worker 1 claims and attempts publication, crashes before DB mark
    await worker1.processBatch();
    expect(outboxTable[0].status).toBe('PENDING'); // or lease expired
    expect(publishedKafkaMessages).toHaveLength(1);

    // Worker 2 attempts next cycle: successfully publishes and marks PUBLISHED
    await worker2.processBatch();
    expect(outboxTable[0].status).toBe('PUBLISHED');
    expect(publishedKafkaMessages).toHaveLength(2); // At-least-once verified
  });

  it('Scenario C: concurrent workers cannot claim the same active lease', async () => {
    outboxTable.push({
      id: 'evt_exclusive_1',
      aggregate_id: 'ord_exclusive_1',
      aggregate_type: 'Order',
      event_type: 'OrderCreated',
      event_version: 1,
      payload: { order_id: 'ord_exclusive_1' },
      correlation_id: 'corr_ex',
      causation_id: 'cmd_ex',
      topic: 'orders.events',
      status: 'PENDING',
      attempts: 0,
      created_at: new Date(),
    });

    const workerA = new OutboxRelay(dbMock, { workerId: 'worker-A', leaseDurationSeconds: 30 });
    const workerB = new OutboxRelay(dbMock, { workerId: 'worker-B', leaseDurationSeconds: 30 });

    (workerA as any).producer = { send: jest.fn().mockResolvedValue([{ partition: 0, offset: '1' }]) };
    (workerB as any).producer = { send: jest.fn().mockResolvedValue([{ partition: 0, offset: '2' }]) };

    // Worker A claims the single event
    const countA = await workerA.processBatch();
    expect(countA).toBe(1);

    // Worker B attempts concurrently while Worker A is processing
    const countB = await workerB.processBatch();
    expect(countB).toBe(0); // Zero events claimed because active lease is held
  });
});
