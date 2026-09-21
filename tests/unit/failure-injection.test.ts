import { OutboxRelay } from '../../src/infrastructure/outbox/outbox-relay';
import { ProjectionConsumer } from '../../src/application/projections/projection-consumer';
import { SagaOrchestrator } from '../../src/saga/saga-orchestrator';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';

describe('Deterministic Distributed Failure Windows & Injection Tests', () => {
  describe('Window 1: Outbox Process Crash Windows', () => {
    let outboxTable: Map<string, any>;
    let kafkaProduced: any[];
    let dbMock: any;
    let relay: OutboxRelay;

    beforeEach(() => {
      outboxTable = new Map();
      kafkaProduced = [];

      dbMock = {
        getPendingOutboxEvents: jest.fn().mockImplementation(async (limit, workerId) => {
          const claimed: any[] = [];
          for (const [id, row] of outboxTable.entries()) {
            if (row.status === 'PENDING' || (row.status === 'PROCESSING' && row.lease_expires_at < new Date())) {
              row.status = 'PROCESSING';
              row.lease_owner = workerId;
              row.lease_expires_at = new Date(Date.now() + 30000);
              row.attempts = (row.attempts || 0) + 1;
              outboxTable.set(id, row);
              claimed.push(row);
              if (claimed.length >= limit) break;
            }
          }
          return claimed;
        }),
        markOutboxEventPublished: jest.fn().mockImplementation(async (id, workerId) => {
          const row = outboxTable.get(id);
          if (row && row.status === 'PROCESSING' && (!workerId || row.lease_owner === workerId)) {
            row.status = 'PUBLISHED';
            row.lease_owner = null;
            return true;
          }
          return false;
        }),
        markOutboxEventFailed: jest.fn().mockImplementation(async (id, error, workerId) => {
          const row = outboxTable.get(id);
          if (row && row.status === 'PROCESSING' && (!workerId || row.lease_owner === workerId)) {
            row.last_error = error;
            row.status = row.attempts >= 10 ? 'FAILED' : 'PENDING';
            row.lease_owner = null;
            return true;
          }
          return false;
        }),
      };

      relay = new OutboxRelay(dbMock, { workerId: 'worker-crash-test', leaseDurationSeconds: 1 });
      (relay as any).producer = {
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        send: jest.fn().mockImplementation(async (msg) => {
          kafkaProduced.push(msg);
          return [{ topicName: msg.topic, partition: 0, errorCode: 0, offset: '10' }];
        }),
      };
    });

    it('OUTBOX_AFTER_KAFKA_ACK / OUTBOX_BEFORE_DB_MARK: process crash after broker ack is handled by At-Least-Once downstream deduplication', async () => {
      const eventId = 'evt_crash_window_1';
      outboxTable.set(eventId, {
        id: eventId,
        aggregate_id: 'ord_1',
        topic: 'orders.events',
        event_type: 'OrderCreated',
        payload: { order_id: 'ord_1' },
        created_at: new Date(),
        status: 'PENDING',
        attempts: 0,
      });

      // Simulate crash right after Kafka ACK by throwing before markOutboxEventPublished
      (relay as any).producer.send = jest.fn().mockImplementation(async (msg) => {
        kafkaProduced.push(msg);
        throw new Error('SIMULATED_HOST_KILL_BEFORE_DB_MARK');
      });

      await relay.processBatch();

      // 1. Message reached Kafka broker
      expect(kafkaProduced.length).toBe(1);

      // 2. Outbox row failed to mark PUBLISHED, remains recoverable in PENDING/PROCESSING
      const row = outboxTable.get(eventId);
      expect(row.status).toBe('PENDING');
      expect(row.attempts).toBe(1);
    });

    it('OUTBOX_AFTER_LEASE_EXPIRY: stale worker cannot overwrite newly reclaimed worker lease', async () => {
      const eventId = 'evt_lease_expired_window';
      outboxTable.set(eventId, {
        id: eventId,
        aggregate_id: 'ord_2',
        topic: 'orders.events',
        status: 'PROCESSING',
        lease_owner: 'worker-B', // Reclaimed by Worker B
        lease_expires_at: new Date(Date.now() + 20000),
        attempts: 2,
      });

      // Stale Worker A attempts completion
      const staleSuccess = await dbMock.markOutboxEventPublished(eventId, 'worker-A');
      expect(staleSuccess).toBe(false);

      const row = outboxTable.get(eventId);
      expect(row.status).toBe('PROCESSING');
      expect(row.lease_owner).toBe('worker-B');
    });
  });

  describe('Window 2: Projection & Marker Atomicity Windows', () => {
    let orderReadModel: Map<string, any>;
    let projectionApplied: Set<string>;
    let projectionConsumer: ProjectionConsumer;

    beforeEach(() => {
      orderReadModel = new Map();
      projectionApplied = new Set();

      const dbMock: any = {
        isProjectionEventApplied: jest.fn().mockImplementation(async (_proj, eventId) => {
          return projectionApplied.has(eventId);
        }),
        markProjectionEventApplied: jest.fn().mockImplementation(async (_proj, eventId) => {
          projectionApplied.add(eventId);
        }),
        getPool: () => ({
          connect: async () => ({
            query: async (sql: string, params: any[] = []) => {
              if (sql === 'BEGIN' || sql === 'COMMIT' || sql === 'ROLLBACK') return { rows: [] };
              if (sql.includes('INSERT INTO order_read_model')) {
                orderReadModel.set(params[0], { order_id: params[0], status: 'pending', total_amount: params[2] });
              }
              if (sql.includes('projection_applied_events')) {
                projectionApplied.add(params[1]);
              }
              return { rows: [] };
            },
            release: () => {},
          }),
        }),
      };

      projectionConsumer = new ProjectionConsumer(null as any, dbMock);
    });

    it('PROJECTION_BEFORE_MARK / PROJECTION_AFTER_MARK: failure inside transaction rolls back cleanly', async () => {
      const envelope = createEventEnvelope({
        eventType: EventTypes.OrderCreated,
        aggregateId: 'ord_fail_tx',
        aggregateType: 'Order',
        producer: 'order-service',
        correlationId: 'corr_tx',
        causationId: 'cmd_tx',
        payload: { order_id: 'ord_fail_tx', user_id: 'usr_1', total_amount: 100 },
      });

      const clientMock: any = {
        query: jest.fn().mockImplementation(async (sql: string) => {
          if (sql.includes('INSERT INTO order_read_model')) {
            throw new Error('SIMULATED_DB_DISK_FULL_ON_PROJECTION');
          }
          return { rows: [] };
        }),
      };

      await expect(projectionConsumer.applyHistoricalEvent(envelope, clientMock)).rejects.toThrow(
        'SIMULATED_DB_DISK_FULL_ON_PROJECTION'
      );
    });
  });

  describe('Window 3: Saga Compensation Barrier Crash Windows', () => {
    let sagasTable: Map<string, any>;
    let emittedEvents: any[];
    let orchestrator: SagaOrchestrator;

    beforeEach(() => {
      sagasTable = new Map();
      emittedEvents = [];

      const dbMock: any = {
        withTransaction: jest.fn().mockImplementation(async (cb) => {
          return await cb({});
        }),
        getSagaByAggregateId: jest.fn().mockImplementation(async (id) => {
          return sagasTable.get(id) || null;
        }),
        saveSagaInstance: jest.fn().mockImplementation(async (saga) => {
          sagasTable.set(saga.aggregateId, {
            saga_id: saga.sagaId,
            aggregate_id: saga.aggregateId,
            saga_type: saga.sagaType,
            state: saga.state,
            current_step: saga.currentStep,
            correlation_id: saga.correlationId,
            context: saga.context,
            failure_reason: saga.failureReason,
          });
        }),
      };

      orchestrator = new SagaOrchestrator(dbMock);
      (orchestrator as any).emit = jest.fn().mockImplementation(async (params) => {
        emittedEvents.push(params);
      });
    });

    it('SAGA_BEFORE_SAVE / SAGA_AFTER_SAVE: partial compensation maintains barrier in COMPENSATING state', async () => {
      const orderId = 'ord_saga_window_1';
      sagasTable.set(orderId, {
        saga_id: `saga_${orderId}`,
        aggregate_id: orderId,
        saga_type: 'ORDER_FULFILLMENT',
        state: 'COMPENSATING',
        current_step: 'COMPENSATING_FRAUD',
        correlation_id: 'corr_1',
        context: {
          order_id: orderId,
          compensations_pending: ['INVENTORY_RELEASE', 'PAYMENT_REFUND'],
          compensations_completed: [],
        },
      });

      const paymentRefunded = createEventEnvelope({
        eventType: EventTypes.PaymentRefunded,
        aggregateId: orderId,
        aggregateType: 'Payment',
        producer: 'payment-service',
        correlationId: 'corr_1',
        causationId: 'evt_ref',
        payload: { order_id: orderId, payment_id: 'pay_1' },
      });

      await (orchestrator as any).processEvent(paymentRefunded);

      // Saga MUST remain in COMPENSATING and NOT emit OrderCancelled yet
      const current = sagasTable.get(orderId);
      expect(current.state).toBe('COMPENSATING');
      expect(current.context.compensations_completed).toEqual(['PAYMENT_REFUND']);
      expect(emittedEvents.length).toBe(0);
    });
  });
});
