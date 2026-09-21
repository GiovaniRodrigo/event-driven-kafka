import { z } from 'zod';

export const OrderCompletedPayloadSchema = z.object({
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  completed_at: z.string().datetime().or(z.string()),
  total_amount: z.number().positive(),
  payment_id: z.string().optional(),
  shipment_id: z.string().optional(),
  tracking_number: z.string().optional(),
});
export type OrderCompletedPayload = z.infer<typeof OrderCompletedPayloadSchema>;

export const OrderFailedPayloadSchema = z.object({
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  reason: z.string().min(1),
  failed_at: z.string().datetime().or(z.string()),
  failed_step: z.string().min(1),
  compensation_status: z.enum(['COMPENSATED', 'NO_COMPENSATION_NEEDED', 'COMPENSATION_FAILED']),
});
export type OrderFailedPayload = z.infer<typeof OrderFailedPayloadSchema>;
