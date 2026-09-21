import { EventReplayService } from '../../src/replay/event-replay-service';
import { ProjectionConsumer } from '../../src/application/projections/projection-consumer';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';

describe('Event Store to CQRS Read Model Rebuild Verification', () => {
  let dbMock: any;
  let eventStore: any[];
  let orderReadModel: Map<string, any>;
  let paymentReadModel: Map<string, any>;
  let shipmentReadModel: Map<string, any>;
  let inventoryReadModel: Map<string, any>;
  let projectionAppliedEvents: Set<string>;
  let gatewayMock: any;
  let projectionConsumer: ProjectionConsumer;
  let replayService: EventReplayService;

  beforeEach(() => {
    eventStore = [];
    orderReadModel = new Map();
    paymentReadModel = new Map();
    shipmentReadModel = new Map();
    inventoryReadModel = new Map();
    projectionAppliedEvents = new Set();

    inventoryReadModel.set('PHONE-PRO', {
      sku: 'PHONE-PRO',
      name: 'Smartphone Pro',
      total_stock: 50,
      reserved_stock: 0,
      available_stock: 50,
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
          });
        } else if (sql.includes('UPDATE order_read_model')) {
          const matchStatus = sql.match(/status = '([^']+)'/);
          if (matchStatus && params[0]) {
            const row = orderReadModel.get(params[0]) || {};
            row.status = matchStatus[1];
            if (sql.includes('payment_id = $2')) row.payment_id = params[1];
            if (sql.includes('reservation_id = $2')) row.reservation_id = params[1];
            if (sql.includes('fraud_check_id = $2')) row.fraud_check_id = params[1];
            if (sql.includes('shipment_id = $2')) {
              row.shipment_id = params[1];
              row.tracking_number = params[2];
            }
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
              row.available_stock -= qty;
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
      appendToEventStore: jest.fn().mockImplementation(async (env) => {
        eventStore.push(env);
      }),
      getEventsByAggregateId: jest.fn().mockImplementation(async (id) => {
        return eventStore.filter((e) => e.aggregate_id === id).sort((a, b) => (a.sequence_number || 0) - (b.sequence_number || 0));
      }),
      isProjectionEventApplied: jest.fn().mockImplementation(async (p, e) => projectionAppliedEvents.has(`${p}:${e}`)),
      markProjectionEventApplied: jest.fn().mockImplementation(async (p, e) => projectionAppliedEvents.add(`${p}:${e}`)),
      resetReadModelForAggregate: jest.fn().mockImplementation(async (id) => {
        orderReadModel.delete(id);
        paymentReadModel.delete(id);
        shipmentReadModel.delete(id);
        for (const item of Array.from(projectionAppliedEvents)) {
          if (item.includes(id)) projectionAppliedEvents.delete(item);
        }
      }),
      getOrder: jest.fn().mockImplementation(async (id) => orderReadModel.get(id) || null),
      recordEvent: jest.fn().mockResolvedValue(undefined),
    };

    projectionConsumer = new ProjectionConsumer(gatewayMock, dbMock as any);
    replayService = new EventReplayService(dbMock as any, projectionConsumer);
    (replayService as any).producer = { send: jest.fn().mockResolvedValue([]) };
  });

  it('completely rebuilds all read model fields from historical event store with zero loss', async () => {
    const orderId = 'ord_store_100';
    const correlationId = 'corr_store_100';

    // Seed historical events into event store
    eventStore.push(
      createEventEnvelope({
        eventId: 'evt_1',
        eventType: EventTypes.OrderCreated,
        aggregateId: orderId,
        aggregateType: 'Order',
        producer: 'order-service',
        correlationId,
        causationId: 'cmd_1',
        sequenceNumber: 1,
        payload: { order_id: orderId, user_id: 'usr_charlie', total_amount: 899.99, items: [{ sku: 'PHONE-PRO', quantity: 2, price: 449.99 }] },
      }),
      createEventEnvelope({
        eventId: 'evt_2',
        eventType: EventTypes.PaymentAuthorized,
        aggregateId: orderId,
        aggregateType: 'Payment',
        producer: 'payment-service',
        correlationId,
        causationId: 'evt_1',
        sequenceNumber: 2,
        payload: { order_id: orderId, user_id: 'usr_charlie', amount: 899.99, payment_id: 'pay_charlie_01', authorization_code: 'AUTH_899' },
      }),
      createEventEnvelope({
        eventId: 'evt_3',
        eventType: EventTypes.InventoryReserved,
        aggregateId: orderId,
        aggregateType: 'Inventory',
        producer: 'inventory-service',
        correlationId,
        causationId: 'evt_2',
        sequenceNumber: 3,
        payload: { order_id: orderId, reservation_id: 'res_charlie_01', items: [{ sku: 'PHONE-PRO', quantity: 2 }] },
      }),
      createEventEnvelope({
        eventId: 'evt_4',
        eventType: EventTypes.FraudApproved,
        aggregateId: orderId,
        aggregateType: 'Fraud',
        producer: 'fraud-service',
        correlationId,
        causationId: 'evt_3',
        sequenceNumber: 4,
        payload: { order_id: orderId, fraud_check_id: 'fraud_charlie_01', risk_score: 12 },
      }),
      createEventEnvelope({
        eventId: 'evt_5',
        eventType: EventTypes.ShipmentCreated,
        aggregateId: orderId,
        aggregateType: 'Shipment',
        producer: 'shipping-service',
        correlationId,
        causationId: 'evt_4',
        sequenceNumber: 5,
        payload: { order_id: orderId, shipment_id: 'ship_charlie_01', tracking_number: 'TRK_UPS_100', carrier: 'UPS' },
      }),
      createEventEnvelope({
        eventId: 'evt_6',
        eventType: EventTypes.OrderCompleted,
        aggregateId: orderId,
        aggregateType: 'Order',
        producer: 'saga-orchestrator',
        correlationId,
        causationId: 'evt_5',
        sequenceNumber: 6,
        payload: { order_id: orderId, user_id: 'usr_charlie', total_amount: 899.99 },
      })
    );

    // Initial read model is empty
    expect(orderReadModel.get(orderId)).toBeUndefined();
    expect(paymentReadModel.get(orderId)).toBeUndefined();
    expect(shipmentReadModel.get(orderId)).toBeUndefined();

    // Replay from Event Store
    const result = await replayService.replayAggregate(orderId);

    expect(result.status).toBe('SUCCESS');
    expect(result.events_processed).toBe(6);

    // Verify all reconstructed read model fields
    const order = orderReadModel.get(orderId);
    expect(order).toBeDefined();
    expect(order.status).toBe('completed');
    expect(order.total_amount).toBe(899.99);
    expect(order.payment_id).toBe('pay_charlie_01');
    expect(order.reservation_id).toBe('res_charlie_01');
    expect(order.fraud_check_id).toBe('fraud_charlie_01');
    expect(order.shipment_id).toBe('ship_charlie_01');
    expect(order.tracking_number).toBe('TRK_UPS_100');

    // Verify payment read model
    const payment = paymentReadModel.get(orderId);
    expect(payment).toBeDefined();
    expect(payment.payment_id).toBe('pay_charlie_01');
    expect(payment.status).toBe('AUTHORIZED');

    // Verify shipment read model
    const shipment = shipmentReadModel.get(orderId);
    expect(shipment).toBeDefined();
    expect(shipment.tracking_number).toBe('TRK_UPS_100');
    expect(shipment.carrier).toBe('UPS');
  });
});
