import { z } from 'zod';
import { OrderItemSchema } from './order';

export const ShipmentRequestedPayloadSchema = z.object({
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  items: z.array(OrderItemSchema).min(1),
  shipping_address: z.record(z.unknown()).default({}),
});
export type ShipmentRequestedPayload = z.infer<typeof ShipmentRequestedPayloadSchema>;

export const ShipmentCreatedPayloadSchema = z.object({
  shipment_id: z.string().min(1),
  order_id: z.string().min(1),
  tracking_number: z.string().min(1),
  carrier: z.string().default('FEDEX'),
  estimated_delivery: z.string().datetime().or(z.string()),
  created_at: z.string().datetime().or(z.string()),
});
export type ShipmentCreatedPayload = z.infer<typeof ShipmentCreatedPayloadSchema>;

export const ShipmentFailedPayloadSchema = z.object({
  order_id: z.string().min(1),
  reason: z.string().min(1),
  failed_at: z.string().datetime().or(z.string()),
});
export type ShipmentFailedPayload = z.infer<typeof ShipmentFailedPayloadSchema>;
