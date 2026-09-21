import { OrderService } from '../../src/services/order-service';
import { OutboxRelay } from '../../src/infrastructure/outbox/outbox-relay';
import { SagaOrchestrator } from '../../src/saga/saga-orchestrator';
import { PaymentConsumer } from '../../src/consumers/payment-consumer';
import { InventoryConsumer } from '../../src/consumers/inventory-consumer';
import { FraudConsumer } from '../../src/consumers/fraud-consumer';
import { ShippingConsumer } from '../../src/consumers/shipping-consumer';
import { NotificationConsumer } from '../../src/consumers/notification-consumer';
import { ProjectionConsumer } from '../../src/application/projections/projection-consumer';
import { EventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';

describe('E2E Order Fulfillment Pipeline Test (In-Memory Simulator)', () => {
  let dbMock: any;
  let outboxTable: any[];
  let ordersTable: any[];
  let readModelTable: Map<string, any>;
  let sagasTable: Map<string, any>;
  let eventStoreTable: any[];
  let processedEvents: Set<string>;
  let gatewayMock: any;

  beforeEach(() => {
    outboxTable = [];
    ordersTable = [];
    readModelTable = new Map();
    sagasTable = new Map();
    eventStoreTable = [];
    processedEvents = new Set();

    gatewayMock = {
      orderCreated: jest.fn(),
      orderUpdated: jest.fn(),
      orderEvent: jest.fn(),
      consumerHealth: jest.fn(),
    };

    const clientMock = {
      query: jest.fn().mockImplementation(async (sql: string, params: any[] = []) => {
        if (sql.includes('INSERT INTO order_read_model')) {
          readModelTable.set(params[0], { order_id: params[0], user_id: params[1], status: 'pending', total_amount: params[2] });
        } else if (sql.includes('UPDATE order_read_model')) {
          const matchStatus = sql.match(/status = '([^']+)'/);
          if (matchStatus && params[0]) {
            const row = readModelTable.get(params[0]);
            if (row) row.status = matchStatus[1];
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
      withTransaction: jest.fn().mockImplementation(async (cb) => cb({})),
      insertOrder: jest.fn().mockImplementation(async (order) => ordersTable.push(order)),
      insertOutboxEvent: jest.fn().mockImplementation(async (e) => {
        outboxTable.push({
          id: e.id,
          aggregate_id: e.aggregateId,
          aggregate_type: e.aggregateType,
          event_type: e.eventType,
          event_version: e.eventVersion || 1,
          payload: e.payload,
          correlation_id: e.correlationId,
          causation_id: e.causationId,
          topic: e.topic,
          status: 'PENDING',
          attempts: 0,
          created_at: new Date(),
        });
      }),
      getPendingOutboxEvents: jest.fn().mockImplementation(async () => outboxTable.filter((e) => e.status === 'PENDING')),
      markOutboxEventPublished: jest.fn().mockImplementation(async (id) => {
        const item = outboxTable.find((e) => e.id === id);
        if (item) item.status = 'PUBLISHED';
      }),
      isEventProcessed: jest.fn().mockImplementation(async (id, c) => processedEvents.has(`${id}:${c}`)),
      markEventProcessed: jest.fn().mockImplementation(async (id, c) => processedEvents.add(`${id}:${c}`)),
      isProjectionEventApplied: jest.fn().mockResolvedValue(false),
      markProjectionEventApplied: jest.fn().mockResolvedValue(undefined),
      markOutboxEventFailed: jest.fn().mockImplementation(async (id, err) => {
        const item = outboxTable.find((e) => e.id === id);
        if (item) {
          item.attempts++;
          item.last_error = err;
        }
      }),
      saveSagaInstance: jest.fn().mockImplementation(async (s) => sagasTable.set(s.aggregateId, s)),
      getSagaByAggregateId: jest.fn().mockImplementation(async (id) => sagasTable.get(id) || null),
      appendToEventStore: jest.fn().mockImplementation(async (env) => eventStoreTable.push(env)),
      recordEvent: jest.fn().mockResolvedValue(undefined),
    };
  });

  it('executes the complete asynchronous fulfillment chain from OrderCreated to OrderCompleted', async () => {
    const relay = new OutboxRelay(dbMock as any);
    const orderService = new OrderService(dbMock as any, relay);
    const saga = new SagaOrchestrator(dbMock as any);
    const paymentConsumer = new PaymentConsumer(dbMock as any);
    const inventoryConsumer = new InventoryConsumer(dbMock as any);
    const fraudConsumer = new FraudConsumer(dbMock as any);
    const shippingConsumer = new ShippingConsumer(dbMock as any);
    const notificationConsumer = new NotificationConsumer(dbMock as any);
    const projectionConsumer = new ProjectionConsumer(gatewayMock, dbMock as any);

    // Mock direct message dispatch bus
    const dispatchMessage = async (envelope: EventEnvelope) => {
      await (projectionConsumer as any).processEvent(envelope);
      await (saga as any).processEvent(envelope);
    };

    // Attach mock dispatch to producers
    (relay as any).producer = {
      send: jest.fn().mockImplementation(async ({ messages }) => {
        for (const msg of messages) {
          await dispatchMessage(JSON.parse(msg.value));
        }
        return [{ partition: 0, offset: '1' }];
      }),
    };

    const attachDispatch = (consumer: any) => {
      consumer.producer = {
        send: jest.fn().mockImplementation(async ({ messages }) => {
          for (const msg of messages) {
            await dispatchMessage(JSON.parse(msg.value));
          }
          return [{ partition: 0, offset: '1' }];
        }),
      };
    };

    attachDispatch(saga);
    attachDispatch(paymentConsumer);
    attachDispatch(inventoryConsumer);
    attachDispatch(fraudConsumer);
    attachDispatch(shippingConsumer);
    attachDispatch(notificationConsumer);

    // 1. Client creates Order
    const order = await orderService.createOrder('usr_john', [
      { sku: 'LAPTOP-001', name: 'Workstation', price: 1200, quantity: 1 },
    ]);

    expect(order.id).toBeDefined();

    // 2. Outbox Relay publishes OrderCreated -> triggers Saga -> triggers Payment
    await relay.processBatch();

    // 3. PaymentConsumer processes PaymentRequested
    const payRequested = eventStoreTable.find((e) => e.event_type === EventTypes.PaymentRequested);
    expect(payRequested).toBeDefined();
    await (paymentConsumer as any).processEvent(payRequested);

    // 4. InventoryConsumer processes InventoryReservationRequested
    const invRequested = eventStoreTable.find((e) => e.event_type === EventTypes.InventoryReservationRequested);
    expect(invRequested).toBeDefined();
    await (inventoryConsumer as any).processEvent(invRequested);

    // 5. FraudConsumer processes FraudCheckRequested
    const fraudRequested = eventStoreTable.find((e) => e.event_type === EventTypes.FraudCheckRequested);
    expect(fraudRequested).toBeDefined();
    await (fraudConsumer as any).processEvent(fraudRequested);

    // 6. ShippingConsumer processes ShipmentRequested
    const shipRequested = eventStoreTable.find((e) => e.event_type === EventTypes.ShipmentRequested);
    expect(shipRequested).toBeDefined();
    await (shippingConsumer as any).processEvent(shipRequested);

    // 7. NotificationConsumer processes NotificationRequested
    const notifRequested = eventStoreTable.find((e) => e.event_type === EventTypes.NotificationRequested);
    expect(notifRequested).toBeDefined();
    await (notificationConsumer as any).processEvent(notifRequested);

    // 8. Verify terminal Saga & CQRS read model state
    const finalSaga = sagasTable.get(order.id);
    expect(finalSaga?.state).toBe('COMPLETED');

    const orderCompleted = eventStoreTable.find((e) => e.event_type === EventTypes.OrderCompleted);
    expect(orderCompleted).toBeDefined();
    expect(orderCompleted.payload.tracking_number).toBeDefined();
  });
});
