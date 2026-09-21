import { SagaOrchestrator } from '../../src/saga/saga-orchestrator';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';

describe('Saga Compensation Barrier & State Transition Guard Verification', () => {
  let dbMock: any;
  let sagasTable: Map<string, any>;
  let emittedEvents: any[];
  let orchestrator: SagaOrchestrator;

  beforeEach(() => {
    sagasTable = new Map();
    emittedEvents = [];

    dbMock = {
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
      getSagaByAggregateId: jest.fn().mockImplementation(async (aggregateId) => {
        return sagasTable.get(aggregateId) || null;
      }),
      isEventProcessed: jest.fn().mockResolvedValue(false),
      markEventProcessed: jest.fn().mockResolvedValue(undefined),
    };

    orchestrator = new SagaOrchestrator(dbMock);
    (orchestrator as any).emit = jest.fn().mockImplementation(async (params) => {
      emittedEvents.push(params);
      return params;
    });
  });

  it('Scenario 1: FraudRejected dual compensation barrier — PaymentRefunded first, InventoryReleased second', async () => {
    const orderId = 'ord_barrier_01';
    const correlationId = 'corr_01';

    // Seed Saga in FRAUD_PENDING state
    sagasTable.set(orderId, {
      saga_id: `saga_${orderId}`,
      aggregate_id: orderId,
      saga_type: 'ORDER_FULFILLMENT',
      state: 'FRAUD_PENDING',
      current_step: 'FRAUD_CHECK',
      correlation_id: correlationId,
      context: {
        order_id: orderId,
        user_id: 'usr_1',
        total_amount: 1500,
        payment_id: 'pay_01',
        reservation_id: 'res_01',
      },
    });

    // 1. Fraud Rejected -> triggers dual compensations
    const fraudRejected = createEventEnvelope({
      eventType: EventTypes.FraudRejected,
      aggregateId: orderId,
      aggregateType: 'Fraud',
      producer: 'fraud-service',
      correlationId,
      causationId: 'evt_fraud',
      payload: { order_id: orderId, reason: 'Risk threshold exceeded' },
    });
    await (orchestrator as any).processEvent(fraudRejected);

    let currentSaga = sagasTable.get(orderId);
    expect(currentSaga.state).toBe('COMPENSATING');
    expect(currentSaga.context.compensations_pending).toEqual(['INVENTORY_RELEASE', 'PAYMENT_REFUND']);
    expect(currentSaga.context.compensations_completed).toEqual([]);

    // 2. Compensation Step 1 resolves: PaymentRefunded
    const paymentRefunded = createEventEnvelope({
      eventType: EventTypes.PaymentRefunded,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_refund',
      payload: { order_id: orderId, payment_id: 'pay_01', refund_id: 'ref_01' },
    });
    await (orchestrator as any).processEvent(paymentRefunded);

    // State MUST STILL BE COMPENSATING because INVENTORY_RELEASE is not done yet!
    currentSaga = sagasTable.get(orderId);
    expect(currentSaga.state).toBe('COMPENSATING');
    expect(currentSaga.context.compensations_completed).toEqual(['PAYMENT_REFUND']);

    // Verify OrderCancelled was NOT emitted yet
    const orderCancelledPremature = emittedEvents.find((e) => e.eventType === EventTypes.OrderCancelled);
    expect(orderCancelledPremature).toBeUndefined();

    // 3. Compensation Step 2 resolves: InventoryReleased
    const inventoryReleased = createEventEnvelope({
      eventType: EventTypes.InventoryReleased,
      aggregateId: orderId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId,
      causationId: 'evt_rel',
      payload: { order_id: orderId, reservation_id: 'res_01', items: [] },
    });
    await (orchestrator as any).processEvent(inventoryReleased);

    // State NOW transitions to CANCELLED!
    currentSaga = sagasTable.get(orderId);
    expect(currentSaga.state).toBe('CANCELLED');
    expect(currentSaga.current_step).toBe('TERMINAL_CANCELLED');
    expect(currentSaga.context.compensations_completed).toContain('INVENTORY_RELEASE');
    expect(currentSaga.context.compensations_completed).toContain('PAYMENT_REFUND');

    const orderCancelled = emittedEvents.find((e) => e.eventType === EventTypes.OrderCancelled);
    expect(orderCancelled).toBeDefined();
    expect(orderCancelled.payload.order_id).toBe(orderId);
  });

  it('Scenario 2: FraudRejected dual compensation barrier — InventoryReleased first, PaymentRefunded second', async () => {
    const orderId = 'ord_barrier_02';
    const correlationId = 'corr_02';

    sagasTable.set(orderId, {
      saga_id: `saga_${orderId}`,
      aggregate_id: orderId,
      saga_type: 'ORDER_FULFILLMENT',
      state: 'FRAUD_PENDING',
      current_step: 'FRAUD_CHECK',
      correlation_id: correlationId,
      context: { order_id: orderId, payment_id: 'pay_02', reservation_id: 'res_02', total_amount: 300 },
    });

    const fraudRejected = createEventEnvelope({
      eventType: EventTypes.FraudRejected,
      aggregateId: orderId,
      aggregateType: 'Fraud',
      producer: 'fraud-service',
      correlationId,
      causationId: 'evt_f2',
      payload: { order_id: orderId, reason: 'Suspicious IP' },
    });
    await (orchestrator as any).processEvent(fraudRejected);

    // Inventory Released resolves first
    const inventoryReleased = createEventEnvelope({
      eventType: EventTypes.InventoryReleased,
      aggregateId: orderId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId,
      causationId: 'evt_rel2',
      payload: { order_id: orderId, reservation_id: 'res_02', items: [] },
    });
    await (orchestrator as any).processEvent(inventoryReleased);

    let currentSaga = sagasTable.get(orderId);
    expect(currentSaga.state).toBe('COMPENSATING');
    expect(currentSaga.context.compensations_completed).toEqual(['INVENTORY_RELEASE']);

    // Payment Refunded resolves second
    const paymentRefunded = createEventEnvelope({
      eventType: EventTypes.PaymentRefunded,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_ref2',
      payload: { order_id: orderId, payment_id: 'pay_02' },
    });
    await (orchestrator as any).processEvent(paymentRefunded);

    currentSaga = sagasTable.get(orderId);
    expect(currentSaga.state).toBe('CANCELLED');
  });

  it('Scenario 3 & 4: duplicate or late compensation events do not corrupt CANCELLED state', async () => {
    const orderId = 'ord_barrier_03';
    const correlationId = 'corr_03';

    // Saga already completed cancellation
    sagasTable.set(orderId, {
      saga_id: `saga_${orderId}`,
      aggregate_id: orderId,
      saga_type: 'ORDER_FULFILLMENT',
      state: 'CANCELLED',
      current_step: 'TERMINAL_CANCELLED',
      correlation_id: correlationId,
      context: { order_id: orderId, compensations_completed: ['INVENTORY_RELEASE', 'PAYMENT_REFUND'] },
    });

    const latePaymentRefunded = createEventEnvelope({
      eventType: EventTypes.PaymentRefunded,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_late',
      payload: { order_id: orderId, payment_id: 'pay_03' },
    });

    await (orchestrator as any).processEvent(latePaymentRefunded);

    // State remains CANCELLED, emit was not called again
    const currentSaga = sagasTable.get(orderId);
    expect(currentSaga.state).toBe('CANCELLED');
    expect(emittedEvents).toHaveLength(0);
  });
});
