import { z } from 'zod';

/**
 * Shared contracts for the real-time layer. These zod schemas are the single
 * source of truth for the Socket.IO payloads and infer the TypeScript types
 * used on both the producing (server) and, eventually, consuming (dashboard)
 * side. (The spec calls for extracting this into a `contracts` workspace
 * package alongside the frontend; for now it lives in the backend.)
 */

export const orderStatusSchema = z.enum([
  'pending',
  'payment_processing',
  'payment_approved',
  'inventory_reserved',
  'completed',
  'failed',
]);

export const orderSummarySchema = z.object({
  order_id: z.string(),
  user_id: z.string(),
  status: orderStatusSchema,
  total_amount: z.number(),
});
export type OrderSummary = z.infer<typeof orderSummarySchema>;

export const orderUpdateSchema = z.object({
  order_id: z.string(),
  status: orderStatusSchema,
});
export type OrderUpdate = z.infer<typeof orderUpdateSchema>;

export const orderEventPayloadSchema = z.object({
  event_type: z.string(),
  topic: z.string(),
  timestamp: z.string(),
});
export type OrderEventPayload = z.infer<typeof orderEventPayloadSchema>;

export const consumerHealthSchema = z.enum(['healthy', 'degraded', 'down']);

export const consumerHealthMapSchema = z.object({
  payment: consumerHealthSchema,
  inventory: consumerHealthSchema,
  notification: consumerHealthSchema,
});
export type ConsumerHealthMap = z.infer<typeof consumerHealthMapSchema>;

/** Socket.IO event names emitted by the server. */
export const SOCKET_EVENTS = {
  orderCreated: 'order:created',
  orderUpdated: 'order:updated',
  orderEvent: 'order:event',
  consumerHealth: 'consumer:health',
} as const;
