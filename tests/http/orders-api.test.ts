import request from 'supertest';
import { createApp, AppDependencies } from '../../src/app';

function buildDeps(overrides: Partial<AppDependencies> = {}): AppDependencies {
  return {
    orderService: {
      createOrder: jest.fn().mockResolvedValue({
        id: 'ord_new',
        total_amount: 100,
        status: 'pending',
        created_at: new Date('2026-01-01T00:00:00Z'),
      }),
    } as any,
    db: {
      ping: jest.fn().mockResolvedValue(true),
      listOrders: jest.fn().mockResolvedValue([]),
      getOrder: jest.fn().mockResolvedValue(null),
      getOrderEvents: jest.fn().mockResolvedValue([]),
      getEventsByAggregateId: jest.fn().mockResolvedValue([]),
      getMetrics: jest.fn().mockResolvedValue({ total_orders: 1, completed_orders: 1, failed_orders: 0, avg_order_value: 100 }),
      listSagas: jest.fn().mockResolvedValue([]),
      getSagaInstance: jest.fn().mockResolvedValue(null),
      getSagaByAggregateId: jest.fn().mockResolvedValue(null),
      listDLQMessages: jest.fn().mockResolvedValue([]),
      getDLQMessage: jest.fn().mockResolvedValue(null),
    } as any,
    replayService: {
      replayAggregate: jest.fn().mockResolvedValue({ replay_id: 'rpl_1', events_processed: 2, status: 'SUCCESS' }),
      replayDLQ: jest.fn().mockResolvedValue({ replay_id: 'dlq_1', events_processed: 1, status: 'SUCCESS' }),
    } as any,
    getConsumersStatus: jest.fn().mockReturnValue({
      payment: { status: 'healthy', processed: 5 },
      inventory: { status: 'healthy', processed: 5 },
      fraud: { status: 'healthy', processed: 5 },
      shipping: { status: 'healthy', processed: 5 },
      notification: { status: 'healthy', processed: 5 },
      saga: { status: 'healthy', processed: 5 },
      projection: { status: 'healthy', processed: 5 },
    }),
    isKafkaReady: jest.fn().mockResolvedValue(true),
    ...overrides,
  };
}

describe('Full HTTP REST API Tests', () => {
  it('POST /orders rejects invalid payloads with 400', async () => {
    const deps = buildDeps();
    const res = await request(createApp(deps)).post('/orders').send({ items: [] });
    expect(res.status).toBe(400);
  });

  it('POST /orders accepts a valid order with 202 and returns order_id', async () => {
    const deps = buildDeps();
    const res = await request(createApp(deps))
      .post('/orders')
      .send({ user_id: 'user_1', items: [{ sku: 'A', name: 'A', price: 10, quantity: 1 }] });
    expect(res.status).toBe(202);
    expect(res.body).toMatchObject({ order_id: 'ord_new', status: 'pending' });
  });

  it('GET /orders returns recent orders', async () => {
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
          },
        ]),
      } as any,
    });
    const res = await request(createApp(deps)).get('/orders');
    expect(res.status).toBe(200);
    expect(res.body.orders).toHaveLength(1);
  });

  it('GET /orders/:id returns order and timeline', async () => {
    const deps = buildDeps({
      db: {
        ...buildDeps().db,
        getOrder: jest.fn().mockResolvedValue({
          id: 'ord_1',
          user_id: 'u1',
          status: 'completed',
          total_amount: 20,
          items: [],
          created_at: new Date('2026-01-01T00:00:00Z'),
          updated_at: new Date('2026-01-01T00:00:00Z'),
        }),
        getOrderEvents: jest.fn().mockResolvedValue([
          { event_type: 'OrderCreated', topic: 'order-service', timestamp: '2026-01-01T00:00:00Z' },
        ]),
      } as any,
    });
    const res = await request(createApp(deps)).get('/orders/ord_1');
    expect(res.status).toBe(200);
    expect(res.body.order_id).toBe('ord_1');
    expect(res.body.events).toHaveLength(1);
  });

  it('GET /health and GET /ready report service status correctly', async () => {
    const deps = buildDeps();
    const healthRes = await request(createApp(deps)).get('/health');
    expect(healthRes.status).toBe(200);
    expect(healthRes.body.status).toBe('ok');

    const readyRes = await request(createApp(deps)).get('/ready');
    expect(readyRes.status).toBe(200);
    expect(readyRes.body.ready).toBe(true);
  });

  it('GET /metrics returns aggregated operational metrics', async () => {
    const deps = buildDeps();
    const res = await request(createApp(deps)).get('/metrics');
    expect(res.status).toBe(200);
    expect(res.body.total_orders).toBe(1);
  });

  it('GET /consumers returns live consumer metrics', async () => {
    const deps = buildDeps();
    const res = await request(createApp(deps)).get('/consumers');
    expect(res.status).toBe(200);
    expect(res.body.payment.status).toBe('healthy');
  });

  it('POST /replay triggers aggregate event replay', async () => {
    const deps = buildDeps();
    const res = await request(createApp(deps)).post('/replay').send({ aggregate_id: 'ord_1' });
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('SUCCESS');
    expect(deps.replayService?.replayAggregate).toHaveBeenCalledWith('ord_1');
  });

  it('GET /chaos/status and POST /chaos/payment/failure controls fault simulation', async () => {
    const deps = buildDeps();
    const app = createApp(deps);

    const getRes = await request(app).get('/chaos/status');
    expect(getRes.status).toBe(200);

    const setRes = await request(app).post('/chaos/payment/failure').send({ enabled: true });
    expect(setRes.status).toBe(200);
    expect(setRes.body.status.paymentFailure).toBe(true);

    const resetRes = await request(app).post('/chaos/reset');
    expect(resetRes.status).toBe(200);
    expect(resetRes.body.status.paymentFailure).toBe(false);
  });
});
