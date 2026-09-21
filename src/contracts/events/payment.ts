import { z } from 'zod';

export const PaymentRequestedPayloadSchema = z.object({
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  amount: z.number().positive(),
  currency: z.string().default('USD'),
  payment_method: z.string().default('credit_card'),
});
export type PaymentRequestedPayload = z.infer<typeof PaymentRequestedPayloadSchema>;

export const PaymentAuthorizedPayloadSchema = z.object({
  payment_id: z.string().min(1),
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  amount: z.number().positive(),
  authorization_code: z.string().min(1),
  authorized_at: z.string().datetime().or(z.string()),
});
export type PaymentAuthorizedPayload = z.infer<typeof PaymentAuthorizedPayloadSchema>;

export const PaymentRejectedPayloadSchema = z.object({
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  amount: z.number().positive(),
  reason: z.string().min(1),
  rejected_at: z.string().datetime().or(z.string()),
});
export type PaymentRejectedPayload = z.infer<typeof PaymentRejectedPayloadSchema>;

export const PaymentRefundRequestedPayloadSchema = z.object({
  order_id: z.string().min(1),
  payment_id: z.string().optional(),
  amount: z.number().positive(),
  reason: z.string().min(1),
});
export type PaymentRefundRequestedPayload = z.infer<typeof PaymentRefundRequestedPayloadSchema>;

export const PaymentRefundedPayloadSchema = z.object({
  refund_id: z.string().min(1),
  payment_id: z.string().min(1),
  order_id: z.string().min(1),
  amount: z.number().positive(),
  refunded_at: z.string().datetime().or(z.string()),
});
export type PaymentRefundedPayload = z.infer<typeof PaymentRefundedPayloadSchema>;
