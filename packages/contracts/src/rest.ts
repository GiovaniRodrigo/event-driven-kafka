import { z } from 'zod';
import { orderStatusSchema, orderEventPayloadSchema, consumerHealthMapSchema } from './events';

/**
 * REST payload contracts. These mirror exactly what the backend Express routes
 * emit over the wire — note that dates arrive as ISO strings (JSON has no Date
 * type), so every timestamp is `z.string()` here even though the server holds a
 * `Date`.
 */

export const orderItemSchema = z.object({
  sku: z.string(),
  name: z.string(),
  price: z.number(),
  quantity: z.number(),
});
export type OrderItem = z.infer<typeof orderItemSchema>;

/** One row of `GET /orders`. */
export const orderListItemSchema = z.object({
  order_id: z.string(),
  user_id: z.string(),
  status: orderStatusSchema,
  total_amount: z.number(),
  created_at: z.string(),
});
export type OrderListItem = z.infer<typeof orderListItemSchema>;

export const orderListResponseSchema = z.object({
  orders: z.array(orderListItemSchema),
});
export type OrderListResponse = z.infer<typeof orderListResponseSchema>;

/** `GET /orders/:order_id` — the full record plus its event history. */
export const orderDetailSchema = z.object({
  order_id: z.string(),
  user_id: z.string(),
  status: orderStatusSchema,
  items: z.array(orderItemSchema),
  total_amount: z.number(),
  created_at: z.string(),
  updated_at: z.string(),
  events: z.array(orderEventPayloadSchema),
});
export type OrderDetail = z.infer<typeof orderDetailSchema>;

/** `GET /health` — service status plus per-consumer health. */
export const healthResponseSchema = z.object({
  status: z.string(),
  timestamp: z.string(),
  version: z.string(),
  database: z.string(),
  consumers: consumerHealthMapSchema,
});
export type HealthResponse = z.infer<typeof healthResponseSchema>;

/** `POST /orders` request body (mirrors the API's 400 validation). */
export const createOrderRequestSchema = z.object({
  user_id: z.string().min(1),
  items: z.array(orderItemSchema).min(1),
});
export type CreateOrderRequest = z.infer<typeof createOrderRequestSchema>;

/** `POST /orders` 202 Accepted body. */
export const createOrderResponseSchema = z.object({
  order_id: z.string(),
  status: z.string(),
  message: z.string(),
  created_at: z.string(),
});
export type CreateOrderResponse = z.infer<typeof createOrderResponseSchema>;

/**
 * `GET /metrics`. Postgres returns COUNT/AVG as strings (and AVG is NULL on an
 * empty table), so the numeric fields are permissive on the wire; the dashboard
 * coerces them for display.
 */
export const metricsResponseSchema = z.object({
  total_orders: z.union([z.string(), z.number()]),
  completed_orders: z.union([z.string(), z.number()]),
  failed_orders: z.union([z.string(), z.number()]),
  avg_order_value: z.union([z.string(), z.number()]).nullable(),
});
export type MetricsResponse = z.infer<typeof metricsResponseSchema>;
