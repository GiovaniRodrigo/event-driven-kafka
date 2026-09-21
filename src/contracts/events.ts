import { z } from 'zod';
export * from './index';

/**
 * Shared contracts for the real-time layer and socket events.
 */

export const orderStatusSchema = z.enum([
  'pending',
  'payment_pending',
  'payment_processing',
  'payment_approved',
  'payment_rejected',
  'inventory_pending',
  'inventory_reserved',
  'inventory_failed',
  'fraud_pending',
  'fraud_approved',
  'fraud_rejected',
  'shipping_pending',
  'shipping_created',
  'shipping_failed',
  'completed',
  'compensating',
  'cancelled',
  'failed',
]);

export const orderSummarySchema = z.object({
  order_id: z.string(),
  user_id: z.string(),
  status: orderStatusSchema,
  total_amount: z.number(),
  created_at: z.string().optional(),
});
export type OrderSummary = z.infer<typeof orderSummarySchema>;

export const orderUpdateSchema = z.object({
  order_id: z.string(),
  status: orderStatusSchema,
  step: z.string().optional(),
  metadata: z.record(z.unknown()).optional(),
});
export type OrderUpdate = z.infer<typeof orderUpdateSchema>;

export const orderEventPayloadSchema = z.object({
  event_type: z.string(),
  topic: z.string(),
  timestamp: z.string(),
  correlation_id: z.string().optional(),
  causation_id: z.string().optional(),
  payload: z.record(z.unknown()).optional(),
});
export type OrderEventPayload = z.infer<typeof orderEventPayloadSchema>;

export const consumerHealthSchema = z.enum(['healthy', 'degraded', 'down']);
export type ConsumerHealth = z.infer<typeof consumerHealthSchema>;

export const consumerHealthMapSchema = z.record(consumerHealthSchema);
export type ConsumerHealthMap = Record<string, ConsumerHealth>;

/** Socket.IO event names emitted by the server. */
export const SOCKET_EVENTS = {
  orderCreated: 'order:created',
  orderUpdated: 'order:updated',
  orderEvent: 'order:event',
  consumerHealth: 'consumer:health',
  metricsUpdate: 'metrics:update',
  sagaUpdate: 'saga:update',
  dlqUpdate: 'dlq:update',
} as const;
