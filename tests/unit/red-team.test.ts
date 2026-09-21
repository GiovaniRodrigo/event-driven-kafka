import { SagaOrchestrator } from '../../src/saga/saga-orchestrator';
import { OutboxRelay } from '../../src/infrastructure/outbox/outbox-relay';
import { EventReplayService } from '../../src/replay/event-replay-service';
import { BaseConsumer } from '../../src/consumers/base-consumer';
import { EventTypes } from '../../src/contracts';
import { createEventEnvelope } from '../../src/contracts/envelope';

describe('Red Team Audit Resilience & Concurrency Verification', () => {
  let dbMock: any;
  let outboxTable: any[];
  let processedEventsTable: any[];
  let sagaTable: any[];
  let dlqTable: any[];

  beforeEach(() => {
    outboxTable = [];
    processedEventsTable = [];
    sagaTable = [];
    dlqTable = [];

    dbMock = {
      withTransaction: jest.fn().mockImplementation(async (cb) => cb({})),
      
      // Outbox
      getPendingOutboxEvents: jest.fn().mockImplementation(async (limit = 50) => {
        // Simulates UPDATE outbox_events SET status = 'PROCESSING' ... FOR UPDATE SKIP LOCKED RETURNING *
        const pending = outboxTable.filter((e) => e.status === 'PENDING').slice(0, limit);
        for (const item of pending) {
          item.status = 'PROCESSING';
        }
        return pending;
      }),
      markOutboxEventPublished: jest.fn().mockImplementation(async (id) => {
        const item = outboxTable.find((e) => e.id === id);
        if (item) item.status = 'PUBLISHED';
      }),
      markOutboxEventFailed: jest.fn().mockImplementation(async (id, err) => {
        const item = outboxTable.find((e) => e.id === id);
        if (item) {
          item.attempts++;
          item.last_error = err;
          if (item.attempts >= 10) item.status = 'FAILED';
          else item.status = 'PENDING';
        }
      }),

      // Idempotency
      isEventProcessed: jest.fn().mockImplementation(async (eventId, consumerName) => {
        return processedEventsTable.some(
          (p) => p.event_id === eventId && p.consumer_name === consumerName && p.status === 'PROCESSED'
        );
      }),
      markEventProcessed: jest.fn().mockImplementation(async (eventId, consumerName, status = 'PROCESSED', error = null) => {
        const existing = processedEventsTable.find(
          (p) => p.event_id === eventId && p.consumer_name === consumerName
        );
        if (existing) {
          existing.status = status;
          existing.error = error;
        } else {
          processedEventsTable.push({
            event_id: eventId,
            consumer_name: consumerName,
            status,
            error,
            processed_at: new Date(),
          });
        }
      }),
      resetProcessedEventForReplay: jest.fn().mockImplementation(async (eventId, consumerName) => {
        const index = processedEventsTable.findIndex(
          (p) => p.event_id === eventId && (!consumerName || p.consumer_name === consumerName)
        );
        if (index !== -1) {
          processedEventsTable.splice(index, 1);
        }
      }),

      // Saga
      getSagaByAggregateId: jest.fn().mockImplementation(async (orderId) => {
        return sagaTable.find((s) => s.aggregate_id === orderId) || null;
      }),
      saveSagaInstance: jest.fn().mockImplementation(async (saga) => {
        const existing = sagaTable.find((s) => s.aggregate_id === saga.aggregateId);
        if (existing) {
          existing.state = saga.state;
          existing.current_step = saga.currentStep;
          existing.context = saga.context;
          existing.failure_reason = saga.failureReason;
        } else {
          sagaTable.push({
            saga_id: saga.sagaId,
            aggregate_id: saga.aggregateId,
            saga_type: saga.sagaType,
            state: saga.state,
            current_step: saga.currentStep,
            context: saga.context,
            failure_reason: saga.failureReason,
          });
        }
      }),

      // DLQ
      insertDLQOutboxEvent: jest.fn().mockResolvedValue(undefined),
      markDLQOutboxPublished: jest.fn().mockResolvedValue(true),
      recordDLQMessage: jest.fn().mockImplementation(async (dlq) => {
        dlqTable.push(dlq);
      }),
      getDLQMessage: jest.fn().mockImplementation(async (id) => {
        const found = dlqTable.find((d) => d.id === id);
        if (!found) return null;
        return {
          id: found.id,
          event_id: found.eventId,
          topic: found.topic,
          consumer_name: found.consumerName,
          payload: found.payload,
          status: 'UNRESOLVED',
        };
      }),
      setDLQStatus: jest.fn().mockImplementation(async (id, status) => {
        const found = dlqTable.find((d) => d.id === id);
        if (found) found.status = status;
      }),
      markDLQResolved: jest.fn().mockImplementation(async (id, status) => {
        const found = dlqTable.find((d) => d.id === id);
        if (found) found.status = status;
      }),
    };
  });

  describe('1. Saga State Machine Guards Against Out-of-Order & Duplicate Events', () => {
    it('should drop out-of-order FraudApproved event if saga is in CREATED state', async () => {
      const orchestrator = new SagaOrchestrator(dbMock);
      const mockEmit = jest.fn().mockResolvedValue({});
      (orchestrator as any).emit = mockEmit;

      // Seed saga in CREATED state
      sagaTable.push({
        saga_id: 'saga_ord_100',
        aggregate_id: 'ord_100',
        saga_type: 'ORDER_FULFILLMENT',
        state: 'CREATED',
        current_step: 'ORDER_CREATED',
        context: { order_id: 'ord_100' },
      });

      // FraudApproved arrives out of order
      const outOfOrderEvent = createEventEnvelope({
        eventType: EventTypes.FraudApproved,
        aggregateId: 'ord_100',
        aggregateType: 'Fraud',
        producer: 'fraud-service',
        correlationId: 'corr_100',
        causationId: 'evt_cmd_100',
        payload: { order_id: 'ord_100', fraud_check_id: 'fraud_100', risk_score: 10 },
      });

      await (orchestrator as any).processEvent(outOfOrderEvent);

      // Verify saga was NOT mutated to SHIPPING_PENDING and no ShipmentRequested was emitted
      const currentSaga = sagaTable.find((s) => s.aggregate_id === 'ord_100');
      expect(currentSaga.state).toBe('CREATED');
      expect(mockEmit).not.toHaveBeenCalled();
    });

    it('should drop duplicate OrderCreated event if saga is already beyond CREATED state', async () => {
      const orchestrator = new SagaOrchestrator(dbMock);
      const mockEmit = jest.fn().mockResolvedValue({});
      (orchestrator as any).emit = mockEmit;

      sagaTable.push({
        saga_id: 'saga_ord_101',
        aggregate_id: 'ord_101',
        saga_type: 'ORDER_FULFILLMENT',
        state: 'INVENTORY_PENDING',
        current_step: 'INVENTORY',
        context: { order_id: 'ord_101', total_amount: 150 },
      });

      const duplicateOrderCreated = createEventEnvelope({
        eventType: EventTypes.OrderCreated,
        aggregateId: 'ord_101',
        aggregateType: 'Order',
        producer: 'order-service',
        correlationId: 'corr_101',
        causationId: 'cmd_101',
        payload: { order_id: 'ord_101', user_id: 'usr_1', total_amount: 150, items: [] },
      });

      await (orchestrator as any).processEvent(duplicateOrderCreated);

      const currentSaga = sagaTable.find((s) => s.aggregate_id === 'ord_101');
      expect(currentSaga.state).toBe('INVENTORY_PENDING');
      expect(mockEmit).not.toHaveBeenCalled();
    });
  });

  describe('2. Outbox Relay Lease Semantics & Parallel Claiming', () => {
    it('concurrent workers should not pick the same outbox events due to atomic lease state transition', async () => {
      // Seed 2 outbox events
      outboxTable.push(
        {
          id: 'evt_outbox_1',
          aggregate_id: 'ord_1',
          aggregate_type: 'Order',
          event_type: 'OrderCreated',
          event_version: 1,
          payload: { order_id: 'ord_1' },
          correlation_id: 'corr_1',
          causation_id: 'cmd_1',
          topic: 'orders.events',
          status: 'PENDING',
          attempts: 0,
          created_at: new Date(),
        },
        {
          id: 'evt_outbox_2',
          aggregate_id: 'ord_2',
          aggregate_type: 'Order',
          event_type: 'OrderCreated',
          event_version: 1,
          payload: { order_id: 'ord_2' },
          correlation_id: 'corr_2',
          causation_id: 'cmd_2',
          topic: 'orders.events',
          status: 'PENDING',
          attempts: 0,
          created_at: new Date(),
        }
      );

      const worker1 = new OutboxRelay(dbMock, { batchSize: 1 });
      const worker2 = new OutboxRelay(dbMock, { batchSize: 1 });

      (worker1 as any).producer = { send: jest.fn().mockResolvedValue([{ partition: 0, offset: '1' }]) };
      (worker2 as any).producer = { send: jest.fn().mockResolvedValue([{ partition: 0, offset: '2' }]) };

      // Worker 1 and Worker 2 both poll in parallel
      const [w1Count, w2Count] = await Promise.all([
        worker1.processBatch(),
        worker2.processBatch(),
      ]);

      expect(w1Count).toBe(1);
      expect(w2Count).toBe(1);

      // Both events were processed and published exactly once, without collisions
      expect(outboxTable[0].status).toBe('PUBLISHED');
      expect(outboxTable[1].status).toBe('PUBLISHED');
    });
  });

  describe('3. DLQ Replay Idempotency Reset Protocol', () => {
    it('replayDLQ should clear prior FAILED processed_events entry and republish to original topic', async () => {
      const replayService = new EventReplayService(dbMock);
      const mockSend = jest.fn().mockResolvedValue([{ partition: 0, offset: '10' }]);
      (replayService as any).producer = { send: mockSend };

      // Record a failed DLQ message
      const originalEnvelope = createEventEnvelope({
        eventId: 'evt_failed_99',
        eventType: EventTypes.OrderCreated,
        aggregateId: 'ord_99',
        aggregateType: 'Order',
        producer: 'order-service',
        correlationId: 'corr_99',
        causationId: 'cmd_99',
        payload: { order_id: 'ord_99', user_id: 'usr_99', total_amount: 50, items: [] },
      });

      dlqTable.push({
        id: 'dlq_99',
        eventId: 'evt_failed_99',
        topic: 'orders.events',
        consumerName: 'payment-consumer',
        payload: { original_event: originalEnvelope },
        status: 'UNRESOLVED',
      });

      // Mark the consumer's processed_events as FAILED
      processedEventsTable.push({
        event_id: 'evt_failed_99',
        consumer_name: 'payment-consumer',
        status: 'FAILED',
        error: 'Network timeout to payment gateway',
        processed_at: new Date(),
      });

      // Trigger DLQ Replay
      const result = await replayService.replayDLQ('dlq_99');

      expect(result.status).toBe('SUCCESS');
      expect(result.events_processed).toBe(1);

      // Verify the FAILED record was deleted from processed_events to unblock consumer re-processing
      const processedRecord = processedEventsTable.find(
        (p) => p.event_id === 'evt_failed_99' && p.consumer_name === 'payment-consumer'
      );
      expect(processedRecord).toBeUndefined();

      // Verify Kafka producer sent message back to orders.events
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: 'orders.events',
          messages: expect.arrayContaining([
            expect.objectContaining({
              key: 'ord_99',
            }),
          ]),
        })
      );

      // Verify DLQ record marked as REPLAYED
      expect(dlqTable[0].status).toBe('REPLAYED');
    });
  });

  describe('4. Scoped Idempotency Isolation Across Distinct Consumer Groups', () => {
    it('allows two different consumer groups to process the same eventId independently', async () => {
      const eventId = 'evt_shared_123';

      // Consumer A (Payment Consumer) processes event
      await dbMock.markEventProcessed(eventId, 'payment-consumer', 'PROCESSED');
      expect(await dbMock.isEventProcessed(eventId, 'payment-consumer')).toBe(true);

      // Consumer B (Notification Consumer) has NOT processed it yet
      expect(await dbMock.isEventProcessed(eventId, 'notification-consumer')).toBe(false);

      // Consumer B processes event
      await dbMock.markEventProcessed(eventId, 'notification-consumer', 'PROCESSED');
      expect(await dbMock.isEventProcessed(eventId, 'notification-consumer')).toBe(true);

      expect(processedEventsTable).toHaveLength(2);
    });
  });

  describe('5. Non-Retryable Malformed Payload DLQ Quarantine', () => {
    class ConcreteTestConsumer extends BaseConsumer {
      public processed: any[] = [];
      protected async processEvent(envelope: any): Promise<void> {
        this.processed.push(envelope);
      }
    }

    it('should catch malformed JSON payload and route to DLQ without crashing consumer loop', async () => {
      const testConsumer = new ConcreteTestConsumer('orders.events', 'test-group', 'test-consumer', dbMock);
      const mockProducerSend = jest.fn().mockResolvedValue([]);
      (testConsumer as any).producer = { send: mockProducerSend };

      const malformedKafkaMessage: any = {
        topic: 'orders.events',
        partition: 0,
        message: {
          offset: '42',
          value: Buffer.from('{ corrupt_json: unquoted_val '),
          timestamp: '1600000000000',
        },
      };

      await (testConsumer as any).handleMessage(malformedKafkaMessage);

      // Domain processEvent must NOT be invoked
      expect(testConsumer.processed).toHaveLength(0);

      // Malformed message must be recorded in DLQ table
      expect(dlqTable).toHaveLength(1);
      expect(dlqTable[0].consumerName).toBe('test-consumer');
      expect(dlqTable[0].topic).toBe('orders.events');
      expect(dlqTable[0].offset).toBe('42');

      // Kafka DLQ topic was notified
      expect(mockProducerSend).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: 'platform.dlq',
        })
      );
    });
  });
});
