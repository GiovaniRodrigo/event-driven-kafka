import { OrderService } from '../../src/services/order-service';
import { OutboxRelay } from '../../src/infrastructure/outbox/outbox-relay';

describe('Transactional Outbox Unit Tests', () => {
  let dbMock: any;
  let outboxTable: any[];
  let ordersTable: any[];

  beforeEach(() => {
    outboxTable = [];
    ordersTable = [];

    dbMock = {
      withTransaction: jest.fn().mockImplementation(async (cb) => {
        return cb({});
      }),
      insertOrder: jest.fn().mockImplementation(async (order) => {
        ordersTable.push(order);
      }),
      insertOutboxEvent: jest.fn().mockImplementation(async (event) => {
        outboxTable.push({
          id: event.id,
          aggregate_id: event.aggregateId,
          aggregate_type: event.aggregateType,
          event_type: event.eventType,
          event_version: event.eventVersion || 1,
          payload: event.payload,
          correlation_id: event.correlationId,
          causation_id: event.causationId,
          topic: event.topic,
          status: 'PENDING',
          attempts: 0,
          created_at: new Date(),
        });
      }),
      getPendingOutboxEvents: jest.fn().mockImplementation(async () => {
        return outboxTable.filter((e) => e.status === 'PENDING');
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
        }
      }),
    };
  });

  it('OrderService atomically writes order aggregate and outbox event within transaction', async () => {
    const orderService = new OrderService(dbMock as any);
    const order = await orderService.createOrder('usr_123', [
      { sku: 'SKU_1', name: 'Product', price: 50, quantity: 2 },
    ]);

    expect(order.id).toBeDefined();
    expect(ordersTable).toHaveLength(1);
    expect(outboxTable).toHaveLength(1);
    expect(outboxTable[0].aggregate_id).toBe(order.id);
    expect(outboxTable[0].event_type).toBe('OrderCreated');
    expect(outboxTable[0].status).toBe('PENDING');
  });

  it('OutboxRelay processes batch of pending events and marks them PUBLISHED', async () => {
    const relay = new OutboxRelay(dbMock as any);
    (relay as any).producer = {
      send: jest.fn().mockResolvedValue([{ partition: 0, offset: '1' }]),
    };

    outboxTable.push({
      id: 'evt_1',
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
    });

    const count = await relay.processBatch();
    expect(count).toBe(1);
    expect(outboxTable[0].status).toBe('PUBLISHED');
    expect((relay as any).producer.send).toHaveBeenCalled();
  });
});
