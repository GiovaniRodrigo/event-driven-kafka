import {
  orderListResponseSchema,
  orderDetailSchema,
  healthResponseSchema,
  metricsResponseSchema,
  createOrderResponseSchema,
  type CreateOrderRequest,
  type OrderListResponse,
  type OrderDetail,
  type HealthResponse,
  type MetricsResponse,
  type CreateOrderResponse,
} from '@kafka-demo/contracts';

/** Base for REST calls; dev proxies `/api` → backend :3000 (see vite.config). */
const API_BASE = import.meta.env.VITE_API_BASE ?? '/api';

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

async function getJson(path: string): Promise<unknown> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new ApiError(res.status, `GET ${path} → ${res.status}`);
  return res.json();
}

/**
 * The REST surface, each response parsed with the shared zod contract so a
 * server/client shape drift surfaces here as a boundary error rather than a
 * silent `undefined` deep in the UI.
 */
export const api = {
  listOrders: async (): Promise<OrderListResponse> =>
    orderListResponseSchema.parse(await getJson('/orders')),

  getOrder: async (id: string): Promise<OrderDetail> =>
    orderDetailSchema.parse(await getJson(`/orders/${id}`)),

  getHealth: async (): Promise<HealthResponse> =>
    healthResponseSchema.parse(await getJson('/health')),

  getMetrics: async (): Promise<MetricsResponse> =>
    metricsResponseSchema.parse(await getJson('/metrics')),

  createOrder: async (body: CreateOrderRequest): Promise<CreateOrderResponse> => {
    const res = await fetch(`${API_BASE}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      // 400 mirrors the API's missing user_id/items validation.
      throw new ApiError(res.status, `POST /orders → ${res.status}`);
    }
    return createOrderResponseSchema.parse(await res.json());
  },
};

/** SWR cache keys, centralized so realtime patches and reads agree. */
export const swrKeys = {
  orders: '/orders',
  order: (id: string) => ['/orders', id] as const,
  health: '/health',
  metrics: '/metrics',
};
