import { EventReplayService } from '../../src/replay/event-replay-service';
import { ProjectionConsumer } from '../../src/application/projections/projection-consumer';
import { createEventEnvelope, EventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';

describe('Deterministic Event Replay & Projection Idempotency Tests', () => {
  let dbMock: any;
  let eventStoreTable: any[];
  let orderReadModel: Map<string, any>;
  let paymentReadModel: Map<string, any>;
  let shipmentReadModel: Map<string, any>;
  let inventoryReadModel: Map<string, any>;
  let projectionAppliedEvents: Map<string, string>;
  let gatewayMock: any;
  let projectionConsumer: ProjectionConsumer;
  let replayService: EventReplayService;

  beforeEach(() => {
    eventStoreTable = [];
    orderReadModel = new Map();
    paymentReadModel = new Map();
    shipmentReadModel = new Map();
    inventoryReadModel = new Map();
    projectionAppliedEvents = new Map();

    // Seed inventory stock
    inventoryReadModel.set('SKU_WORKSTATION', {
      sku: 'SKU_WORKSTATION',
      name: 'Workstation Pro',
      total_stock: 100,
      reserved_stock: 0,
      available_stock: 100,
    });

    gatewayMock = {
      orderCreated: jest.fn(),
      orderUpdated: jest.fn(),
      orderEvent: jest.fn(),
      consumerHealth: jest.fn(),
    };

    const clientMock = {
      query: jest.fn().mockImplementation(async (sql: string, params: any[] = []) => {
        if (sql.includes('INSERT INTO order_read_model')) {
          orderReadModel.set(params[0], {
            order_id: params[0],
            user_id: params[1],
            status: 'pending',
            total_amount: params[2],
            currency: params[3],
            items: JSON.parse(params[4]),
            version: 1,
          });
        } else if (sql.includes('UPDATE order_read_model')) {
          const matchStatus = sql.match(/status = '([^']+)'/);
          if (matchStatus && params[0]) {
            const row = orderReadModel.get(params[0]) || { order_id: params[0], currency: 'USD', items: [], version: 1 };
            row.status = matchStatus[1];
            if (sql.includes('payment_id = $2')) row.payment_id = params[1];
            if (sql.includes('reservation_id = $2')) row.reservation_id = params[1];
            if (sql.includes('fraud_check_id = $2')) row.fraud_check_id = params[1];
            if (sql.includes('shipment_id = $2')) {
              row.shipment_id = params[1];
              row.tracking_number = params[2];
            }
            if (sql.includes('failure_reason = $2')) row.failure_reason = params[1];
            orderReadModel.set(params[0], row);
          }
        } else if (sql.includes('INSERT INTO payment_read_model')) {
          paymentReadModel.set(params[1], {
            payment_id: params[0],
            order_id: params[1],
            user_id: params[2],
            amount: params[3],
            status: 'AUTHORIZED',
            authorization_code: params[4],
          });
        } else if (sql.includes('UPDATE payment_read_model')) {
          const row = paymentReadModel.get(params[0]);
          if (row) {
            row.status = 'REFUNDED';
            row.refund_id = params[1];
          }
        } else if (sql.includes('INSERT INTO shipment_read_model')) {
          shipmentReadModel.set(params[1], {
            shipment_id: params[0],
            order_id: params[1],
            tracking_number: params[2],
            carrier: params[3],
            status: 'CREATED',
          });
        } else if (sql.includes('UPDATE inventory_read_model')) {
          if (sql.includes('reserved_stock = reserved_stock + $1')) {
            const qty = params[0] || 1;
            const sku = params[2];
            const row = inventoryReadModel.get(sku);
            if (row) {
              row.reserved_stock += qty;
              row.available_stock = Math.max(0, row.available_stock - qty);
            }
          } else if (sql.includes('reserved_stock = GREATEST(0, reserved_stock - $1)')) {
            const qty = params[0] || 1;
            const sku = params[2];
            const row = inventoryReadModel.get(sku);
            if (row) {
              row.reserved_stock = Math.max(0, row.reserved_stock - qty);
              row.available_stock += qty;
            }
          }
        }
        return { rows: [] };
      }),
      release: jest.fn(),
    };

    dbMock = {
      getPool: () => ({
        connect: jest.fn().mockResolvedValue(clientMock),
      }),
      appendToEventStore: jest.fn().mockImplementation(async (env: EventEnvelope) => {
        const nextSeq = eventStoreTable.filter((e) => e.aggregate_id === env.aggregate_id).length + 1;
        eventStoreTable.push({ ...env, sequence_number: env.sequence_number || nextSeq });
      }),
      getEventsByAggregateId: jest.fn().mockImplementation(async (aggregateId: string) => {
        return eventStoreTable
          .filter((e) => e.aggregate_id === aggregateId)
          .sort((a, b) => (a.sequence_number || 0) - (b.sequence_number || 0));
      }),
      isProjectionEventApplied: jest.fn().mockImplementation(async (proj: string, eventId: string) => {
        return projectionAppliedEvents.has(`${proj}:${eventId}`);
      }),
      markProjectionEventApplied: jest.fn().mockImplementation(async (proj: string, eventId: string, aggregateId: string) => {
        projectionAppliedEvents.set(`${proj}:${eventId}`, aggregateId);
      }),
      resetReadModelForAggregate: jest.fn().mockImplementation(async (aggregateId: string) => {
        orderReadModel.delete(aggregateId);
        paymentReadModel.delete(aggregateId);
        shipmentReadModel.delete(aggregateId);
        // Clear projection applied event markers for this aggregate
        for (const [key, agg] of Array.from(projectionAppliedEvents.entries())) {
          if (agg === aggregateId) {
            projectionAppliedEvents.delete(key);
          }
        }
      }),
      getOrder: jest.fn().mockImplementation(async (orderId: string) => {
        return orderReadModel.get(orderId) || null;
      }),
      recordEvent: jest.fn().mockResolvedValue(undefined),
    };

    projectionConsumer = new ProjectionConsumer(gatewayMock, dbMock as any);
    replayService = new EventReplayService(dbMock as any, projectionConsumer);
    (replayService as any).producer = { send: jest.fn().mockResolvedValue([]) };
  });

  it('reconstructs identical read model state from event store sequence (Replay Determinism)', async () => {
    const orderId = 'ord_replay_001';
    const correlationId = 'corr_001';

    // 1. Live processing sequence
    const e1 = createEventEnvelope({
      eventId: 'evt_1',
      eventType: EventTypes.OrderCreated,
      aggregateId: orderId,
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId,
      causationId: 'cmd_1',
      sequenceNumber: 1,
      payload: { order_id: orderId, user_id: 'usr_alice', total_amount: 1999.99, items: [{ sku: 'SKU_WORKSTATION', quantity: 1, price: 1999.99 }] },
    });
    const e2 = createEventEnvelope({
      eventId: 'evt_2',
      eventType: EventTypes.PaymentAuthorized,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_1',
      sequenceNumber: 2,
      payload: { order_id: orderId, user_id: 'usr_alice', amount: 1999.99, payment_id: 'pay_001', authorization_code: 'AUTH_OK' },
    });
    const e3 = createEventEnvelope({
      eventId: 'evt_3',
      eventType: EventTypes.InventoryReserved,
      aggregateId: orderId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId,
      causationId: 'evt_2',
      sequenceNumber: 3,
      payload: { order_id: orderId, reservation_id: 'res_001', items: [{ sku: 'SKU_WORKSTATION', quantity: 1 }] },
    });
    const e4 = createEventEnvelope({
      eventId: 'evt_4',
      eventType: EventTypes.FraudApproved,
      aggregateId: orderId,
      aggregateType: 'Fraud',
      producer: 'fraud-service',
      correlationId,
      causationId: 'evt_3',
      sequenceNumber: 4,
      payload: { order_id: orderId, fraud_check_id: 'fraud_001', risk_score: 5 },
    });
    const e5 = createEventEnvelope({
      eventId: 'evt_5',
      eventType: EventTypes.ShipmentCreated,
      aggregateId: orderId,
      aggregateType: 'Shipment',
      producer: 'shipping-service',
      correlationId,
      causationId: 'evt_4',
      sequenceNumber: 5,
      payload: { order_id: orderId, shipment_id: 'ship_001', tracking_number: 'TRK_FEDEX_99', carrier: 'FEDEX' },
    });
    const e6 = createEventEnvelope({
      eventId: 'evt_6',
      eventType: EventTypes.OrderCompleted,
      aggregateId: orderId,
      aggregateType: 'Order',
      producer: 'saga-orchestrator',
      correlationId,
      causationId: 'evt_5',
      sequenceNumber: 6,
      payload: { order_id: orderId, user_id: 'usr_alice', total_amount: 1999.99 },
    });

    // Apply live events
    for (const evt of [e1, e2, e3, e4, e5, e6]) {
      await (projectionConsumer as any).processEvent(evt);
    }

    // 2. Capture live state snapshot
    const liveOrderState = { ...orderReadModel.get(orderId) };
    expect(liveOrderState.status).toBe('completed');
    expect(liveOrderState.payment_id).toBe('pay_001');
    expect(liveOrderState.reservation_id).toBe('res_001');
    expect(liveOrderState.fraud_check_id).toBe('fraud_001');
    expect(liveOrderState.shipment_id).toBe('ship_001');
    expect(liveOrderState.tracking_number).toBe('TRK_FEDEX_99');

    // Verify inventory state
    expect(inventoryReadModel.get('SKU_WORKSTATION').reserved_stock).toBe(1);
    expect(inventoryReadModel.get('SKU_WORKSTATION').available_stock).toBe(99);

    // 3. Delete / Reset the read model to simulate catastrophic loss
    await dbMock.resetReadModelForAggregate(orderId);
    expect(orderReadModel.get(orderId)).toBeUndefined();

    // 4. Execute Replay #1
    const replayResult1 = await replayService.replayAggregate(orderId);
    expect(replayResult1.status).toBe('SUCCESS');
    expect(replayResult1.events_processed).toBe(6);

    // Assert Replay #1 matches initial state exactly
    const rebuiltOrderState1 = { ...orderReadModel.get(orderId) };
    expect(rebuiltOrderState1).toEqual(liveOrderState);

    // 5. Execute Replay #2 (Testing Idempotency & Repeatability)
    const replayResult2 = await replayService.replayAggregate(orderId);
    expect(replayResult2.status).toBe('SUCCESS');
    expect(replayResult2.events_processed).toBe(6);

    const rebuiltOrderState2 = { ...orderReadModel.get(orderId) };
    expect(rebuiltOrderState2).toEqual(rebuiltOrderState1);
  });

  it('reconstructs compensation flow deterministically without side effects', async () => {
    const orderId = 'ord_comp_002';
    const correlationId = 'corr_comp_002';

    // Sequence: OrderCreated -> PaymentAuthorized -> InventoryReservationFailed -> PaymentRefunded -> OrderCancelled
    const e1 = createEventEnvelope({
      eventId: 'evt_c1',
      eventType: EventTypes.OrderCreated,
      aggregateId: orderId,
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId,
      causationId: 'cmd_c1',
      sequenceNumber: 1,
      payload: { order_id: orderId, user_id: 'usr_bob', total_amount: 500, items: [] },
    });
    const e2 = createEventEnvelope({
      eventId: 'evt_c2',
      eventType: EventTypes.PaymentAuthorized,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_c1',
      sequenceNumber: 2,
      payload: { order_id: orderId, user_id: 'usr_bob', amount: 500, payment_id: 'pay_comp_002' },
    });
    const e3 = createEventEnvelope({
      eventId: 'evt_c3',
      eventType: EventTypes.InventoryReservationFailed,
      aggregateId: orderId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId,
      causationId: 'evt_c2',
      sequenceNumber: 3,
      payload: { order_id: orderId, reason: 'Warehouse stock depleted' },
    });
    const e4 = createEventEnvelope({
      eventId: 'evt_c4',
      eventType: EventTypes.PaymentRefunded,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_c3',
      sequenceNumber: 4,
      payload: { order_id: orderId, refund_id: 'ref_002' },
    });
    const e5 = createEventEnvelope({
      eventId: 'evt_c5',
      eventType: EventTypes.OrderCancelled,
      aggregateId: orderId,
      aggregateType: 'Order',
      producer: 'saga-orchestrator',
      correlationId,
      causationId: 'evt_c4',
      sequenceNumber: 5,
      payload: { order_id: orderId, reason: 'Warehouse stock depleted' },
    });

    for (const evt of [e1, e2, e3, e4, e5]) {
      await (projectionConsumer as any).processEvent(evt);
    }

    const originalCompState = { ...orderReadModel.get(orderId) };
    expect(originalCompState.status).toBe('cancelled');
    expect(originalCompState.failure_reason).toBe('Warehouse stock depleted');

    // Reset and Replay
    await dbMock.resetReadModelForAggregate(orderId);
    const replayResult = await replayService.replayAggregate(orderId);

    expect(replayResult.status).toBe('SUCCESS');
    expect(replayResult.events_processed).toBe(5);
    expect(orderReadModel.get(orderId)).toEqual(originalCompState);
  });
});
