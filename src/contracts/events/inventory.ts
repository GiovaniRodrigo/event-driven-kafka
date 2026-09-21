import { z } from 'zod';
import { OrderItemSchema } from './order';

export const InventoryReservationRequestedPayloadSchema = z.object({
  order_id: z.string().min(1),
  items: z.array(OrderItemSchema).min(1),
});
export type InventoryReservationRequestedPayload = z.infer<
  typeof InventoryReservationRequestedPayloadSchema
>;

export const InventoryReservedPayloadSchema = z.object({
  reservation_id: z.string().min(1),
  order_id: z.string().min(1),
  items: z.array(OrderItemSchema).min(1),
  reserved_at: z.string().datetime().or(z.string()),
});
export type InventoryReservedPayload = z.infer<typeof InventoryReservedPayloadSchema>;

export const InventoryReservationFailedPayloadSchema = z.object({
  order_id: z.string().min(1),
  items: z.array(OrderItemSchema).min(1),
  reason: z.string().min(1),
  failed_at: z.string().datetime().or(z.string()),
});
export type InventoryReservationFailedPayload = z.infer<
  typeof InventoryReservationFailedPayloadSchema
>;

export const InventoryReleasedPayloadSchema = z.object({
  reservation_id: z.string().optional(),
  order_id: z.string().min(1),
  items: z.array(OrderItemSchema).min(1),
  released_at: z.string().datetime().or(z.string()),
  reason: z.string().min(1),
});
export type InventoryReleasedPayload = z.infer<typeof InventoryReleasedPayloadSchema>;
