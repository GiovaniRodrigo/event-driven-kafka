import { SagaOrchestrator } from '../../src/saga/saga-orchestrator';
import { EventTypes } from '../../src/contracts';
import { createEventEnvelope } from '../../src/contracts/envelope';

describe('Saga Orchestrator Unit Tests', () => {
  let sagaDbMock: any;
  let orchestrator: SagaOrchestrator;
  let emittedEvents: any[] = [];
  let savedSagas: Map<string, any>;

  beforeEach(() => {
    emittedEvents = [];
    savedSagas = new Map();

    sagaDbMock = {
      isEventProcessed: jest.fn().mockResolvedValue(false),
      markEventProcessed: jest.fn().mockResolvedValue(undefined),
      saveSagaInstance: jest.fn().mockImplementation(async (saga) => {
        savedSagas.set(saga.aggregateId, {
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
      getSagaByAggregateId: jest.fn().mockImplementation(async (orderId) => {
        return savedSagas.get(orderId) || null;
      }),
    };

    orchestrator = new SagaOrchestrator(sagaDbMock as any);
    // Mock the emit method to capture downstream events
    (orchestrator as any).emit = jest.fn().mockImplementation(async (params) => {
      emittedEvents.push(params);
    });
  });

  it('progresses happy path through Payment -> Inventory -> Fraud -> Shipping -> Completed', async () => {
    const orderId = 'ord_test_1';
    const correlationId = 'corr_test_1';

    // Step 1: OrderCreated
    const orderCreated = createEventEnvelope({
      eventType: EventTypes.OrderCreated,
      aggregateId: orderId,
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId,
      causationId: 'cmd_1',
      payload: {
        order_id: orderId,
        user_id: 'usr_1',
        total_amount: 100,
        items: [{ sku: 'SKU_1', name: 'Item', price: 100, quantity: 1 }],
      },
    });
    await (orchestrator as any).processEvent(orderCreated);

    expect(savedSagas.get(orderId)?.state).toBe('PAYMENT_PENDING');
    expect(emittedEvents[0].eventType).toBe(EventTypes.PaymentRequested);

    // Step 2: PaymentAuthorized
    const paymentAuthorized = createEventEnvelope({
      eventType: EventTypes.PaymentAuthorized,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_pay_req',
      payload: {
        payment_id: 'pay_1',
        order_id: orderId,
        user_id: 'usr_1',
        amount: 100,
        authorization_code: 'AUTH_123',
        authorized_at: new Date().toISOString(),
      },
    });
    await (orchestrator as any).processEvent(paymentAuthorized);

    expect(savedSagas.get(orderId)?.state).toBe('INVENTORY_PENDING');
    expect(emittedEvents[1].eventType).toBe(EventTypes.InventoryReservationRequested);

    // Step 3: InventoryReserved
    const inventoryReserved = createEventEnvelope({
      eventType: EventTypes.InventoryReserved,
      aggregateId: orderId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId,
      causationId: 'evt_inv_req',
      payload: {
        reservation_id: 'res_1',
        order_id: orderId,
        items: [{ sku: 'SKU_1', name: 'Item', price: 100, quantity: 1 }],
        reserved_at: new Date().toISOString(),
      },
    });
    await (orchestrator as any).processEvent(inventoryReserved);

    expect(savedSagas.get(orderId)?.state).toBe('FRAUD_PENDING');
    expect(emittedEvents[2].eventType).toBe(EventTypes.FraudCheckRequested);

    // Step 4: FraudApproved
    const fraudApproved = createEventEnvelope({
      eventType: EventTypes.FraudApproved,
      aggregateId: orderId,
      aggregateType: 'Fraud',
      producer: 'fraud-service',
      correlationId,
      causationId: 'evt_fraud_req',
      payload: {
        fraud_check_id: 'fraud_1',
        order_id: orderId,
        risk_score: 15,
        approved_at: new Date().toISOString(),
      },
    });
    await (orchestrator as any).processEvent(fraudApproved);

    expect(savedSagas.get(orderId)?.state).toBe('SHIPPING_PENDING');
    expect(emittedEvents[3].eventType).toBe(EventTypes.ShipmentRequested);

    // Step 5: ShipmentCreated -> Terminal COMPLETED
    const shipmentCreated = createEventEnvelope({
      eventType: EventTypes.ShipmentCreated,
      aggregateId: orderId,
      aggregateType: 'Shipment',
      producer: 'shipping-service',
      correlationId,
      causationId: 'evt_ship_req',
      payload: {
        shipment_id: 'ship_1',
        order_id: orderId,
        tracking_number: 'TRK-999',
        carrier: 'FEDEX',
        estimated_delivery: new Date().toISOString(),
        created_at: new Date().toISOString(),
      },
    });
    await (orchestrator as any).processEvent(shipmentCreated);

    expect(savedSagas.get(orderId)?.state).toBe('COMPLETED');
    const orderCompletedEvent = emittedEvents.find((e) => e.eventType === EventTypes.OrderCompleted);
    expect(orderCompletedEvent).toBeDefined();
    expect(orderCompletedEvent.payload.tracking_number).toBe('TRK-999');
  });

  it('triggers compensation when InventoryReservationFailed occurs', async () => {
    const orderId = 'ord_failed_inv';
    const correlationId = 'corr_failed_inv';

    // Start Saga & Authorize Payment
    const orderCreated = createEventEnvelope({
      eventType: EventTypes.OrderCreated,
      aggregateId: orderId,
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId,
      causationId: 'cmd_1',
      payload: { order_id: orderId, user_id: 'usr_1', total_amount: 100, items: [] },
    });
    await (orchestrator as any).processEvent(orderCreated);

    const paymentAuthorized = createEventEnvelope({
      eventType: EventTypes.PaymentAuthorized,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_1',
      payload: { payment_id: 'pay_refund_me', order_id: orderId, user_id: 'usr_1', amount: 100, authorization_code: 'AUTH', authorized_at: new Date().toISOString() },
    });
    await (orchestrator as any).processEvent(paymentAuthorized);

    // Inventory Fails
    const inventoryFailed = createEventEnvelope({
      eventType: EventTypes.InventoryReservationFailed,
      aggregateId: orderId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId,
      causationId: 'evt_2',
      payload: { order_id: orderId, items: [], reason: 'Out of stock', failed_at: new Date().toISOString() },
    });
    await (orchestrator as any).processEvent(inventoryFailed);

    expect(savedSagas.get(orderId)?.state).toBe('COMPENSATING');
    const refundRequested = emittedEvents.find((e) => e.eventType === EventTypes.PaymentRefundRequested);
    expect(refundRequested).toBeDefined();
    expect(refundRequested.payload.payment_id).toBe('pay_refund_me');

    // Payment Refund Resolves
    const paymentRefunded = createEventEnvelope({
      eventType: EventTypes.PaymentRefunded,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_refund',
      payload: { refund_id: 'ref_1', payment_id: 'pay_refund_me', order_id: orderId, amount: 100, refunded_at: new Date().toISOString() },
    });
    await (orchestrator as any).processEvent(paymentRefunded);

    expect(savedSagas.get(orderId)?.state).toBe('CANCELLED');
    const orderCancelled = emittedEvents.find((e) => e.eventType === EventTypes.OrderCancelled);
    expect(orderCancelled).toBeDefined();
  });
});
