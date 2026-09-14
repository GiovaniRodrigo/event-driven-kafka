import { setupServer } from 'msw/node';
import { http, HttpResponse } from 'msw';
import type {
  HealthResponse,
  MetricsResponse,
  OrderDetail,
  OrderListResponse,
} from '@kafka-demo/contracts';

export const mockOrders: OrderListResponse = {
  orders: [
    { order_id: 'ord_completed01', user_id: 'user-1', status: 'completed', total_amount: 149.9, created_at: new Date().toISOString() },
    { order_id: 'ord_failed02', user_id: 'user-2', status: 'failed', total_amount: 42.25, created_at: new Date().toISOString() },
  ],
};

export const mockHealth: HealthResponse = {
  status: 'ok',
  timestamp: new Date().toISOString(),
  version: '1.0.0',
  database: 'healthy',
  consumers: { payment: 'healthy', inventory: 'degraded', notification: 'healthy' },
};

export const mockMetrics: MetricsResponse = {
  total_orders: '2',
  completed_orders: '1',
  failed_orders: '1',
  avg_order_value: '96.08',
};

export const mockOrderDetail: OrderDetail = {
  order_id: 'ord_completed01',
  user_id: 'user-1',
  status: 'completed',
  items: [{ sku: 'SKU-1', name: 'Widget', price: 74.95, quantity: 2 }],
  total_amount: 149.9,
  created_at: new Date().toISOString(),
  updated_at: new Date().toISOString(),
  events: [
    { event_type: 'order.created', topic: 'orders', timestamp: new Date().toISOString() },
  ],
};

/** Default happy-path handlers; individual tests override with server.use(). */
export const handlers = [
  http.get('*/api/orders', () => HttpResponse.json(mockOrders)),
  http.get('*/api/orders/:id', () => HttpResponse.json(mockOrderDetail)),
  http.get('*/api/health', () => HttpResponse.json(mockHealth)),
  http.get('*/api/metrics', () => HttpResponse.json(mockMetrics)),
  http.post('*/api/orders', () =>
    HttpResponse.json(
      { order_id: 'ord_new123', status: 'pending', message: 'accepted', created_at: new Date().toISOString() },
      { status: 202 },
    ),
  ),
];

export const server = setupServer(...handlers);
