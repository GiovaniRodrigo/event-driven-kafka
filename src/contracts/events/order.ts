import { z } from 'zod';

export const OrderItemSchema = z.object({
  sku: z.string().min(1),
  name: z.string().min(1),
  price: z.number().positive(),
  quantity: z.number().int().positive(),
});
export type OrderItem = z.infer<typeof OrderItemSchema>;

export const OrderCreatedPayloadSchema = z.object({
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  items: z.array(OrderItemSchema).min(1),
  total_amount: z.number().positive(),
  currency: z.string().default('USD'),
  shipping_address: z
    .object({
      street: z.string().optional(),
      city: z.string().optional(),
      zip: z.string().optional(),
      country: z.string().default('US'),
    })
    .default({ country: 'US' }),
});
export type OrderCreatedPayload = z.infer<typeof OrderCreatedPayloadSchema>;

export const OrderCancelledPayloadSchema = z.object({
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  reason: z.string().min(1),
  cancelled_at: z.string().datetime().or(z.string()),
  compensated_steps: z.array(z.string()).default([]),
});
export type OrderCancelledPayload = z.infer<typeof OrderCancelledPayloadSchema>;
