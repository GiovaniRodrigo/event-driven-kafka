import request from 'supertest';
import { createApp, AppDependencies } from '../../../src/app';

function buildDeps(overrides: Partial<AppDependencies> = {}): AppDependencies {
  return {
    orderService: {
      createOrder: jest.fn().mockResolvedValue({ id: 'ord_new', created_at: new Date('2026-01-01T00:00:00Z') }),
    },
    db: {
      ping: jest.fn().mockResolvedValue(true),
      listOrders: jest.fn().mockResolvedValue([]),
      getOrder: jest.fn().mockResolvedValue(null),
      getOrderEvents: jest.fn().mockResolvedValue([]),
      getMetrics: jest.fn().mockResolvedValue({ total_orders: '0' }),
    },
    getConsumerHealth: jest.fn().mockReturnValue({
      payment: 'healthy',
      inventory: 'healthy',
      notification: 'healthy',
    }),
    ...overrides,
  };
}

describe('orders API', () => {
  it('POST /orders rejects a payload missing user_id/items with 400', async () => {
    const deps = buildDeps();
    const res = await request(createApp(deps)).post('/orders').send({ items: [] });
    expect(res.status).toBe(400);
    expect(deps.orderService.createOrder).not.toHaveBeenCalled();
  });

  it('POST /orders accepts a valid order with 202 and the new order id', async () => {
    const deps = buildDeps();
    const res = await request(createApp(deps))
      .post('/orders')
      .send({ user_id: 'user_1', items: [{ sku: 'A', name: 'A', price: 10, quantity: 1 }] });
    expect(res.status).toBe(202);
    expect(res.body).toMatchObject({ order_id: 'ord_new', status: 'pending' });
    expect(deps.orderService.createOrder).toHaveBeenCalledWith('user_1', [
      { sku: 'A', name: 'A', price: 10, quantity: 1 },
    ]);
  });

  it('GET /orders returns the recent orders list', async () => {
    const deps = buildDeps({
      db: {
        ...buildDeps().db,
        listOrders: jest.fn().mockResolvedValue([
          {
            id: 'ord_1',
            user_id: 'u1',
            status: 'completed',
            total_amount: 20,
            items: [],
            created_at: new Date('2026-01-01T00:00:00Z'),
            updated_at: new Date('2026-01-01T00:00:00Z'),
          },
        ]),
      },
    });
    const res = await request(createApp(deps)).get('/orders');
    expect(res.status).toBe(200);
    expect(res.body.orders).toHaveLength(1);
    expect(res.body.orders[0]).toMatchObject({ order_id: 'ord_1', status: 'completed' });
  });

  it('GET /orders/:id returns the order with its events timeline', async () => {
    const deps = buildDeps({
      db: {
        ...buildDeps().db,
        getOrder: jest.fn().mockResolvedValue({
          id: 'ord_1',
          user_id: 'u1',
          status: 'completed',
          total_amount: 20,
          items: [{ sku: 'A', name: 'A', price: 20, quantity: 1 }],
          created_at: new Date('2026-01-01T00:00:00Z'),
          updated_at: new Date('2026-01-01T00:00:00Z'),
        }),
        getOrderEvents: jest.fn().mockResolvedValue([
          { event_type: 'order.created', topic: 'orders', timestamp: '2026-01-01T00:00:00.000Z' },
        ]),
      },
    });
    const res = await request(createApp(deps)).get('/orders/ord_1');
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ order_id: 'ord_1', status: 'completed' });
    expect(res.body.events).toHaveLength(1);
    expect(res.body.items).toHaveLength(1);
  });

  it('GET /orders/:id returns 404 when the order does not exist', async () => {
    const res = await request(createApp(buildDeps())).get('/orders/missing');
    expect(res.status).toBe(404);
  });

  it('GET /health reports database and per-consumer status', async () => {
    const res = await request(createApp(buildDeps())).get('/health');
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      database: 'healthy',
      consumers: { payment: 'healthy', inventory: 'healthy', notification: 'healthy' },
    });
  });
});
