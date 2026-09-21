import { z } from 'zod';

export const FraudCheckRequestedPayloadSchema = z.object({
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  amount: z.number().positive(),
  payment_id: z.string().min(1),
});
export type FraudCheckRequestedPayload = z.infer<typeof FraudCheckRequestedPayloadSchema>;

export const FraudApprovedPayloadSchema = z.object({
  fraud_check_id: z.string().min(1),
  order_id: z.string().min(1),
  risk_score: z.number().min(0).max(100),
  approved_at: z.string().datetime().or(z.string()),
});
export type FraudApprovedPayload = z.infer<typeof FraudApprovedPayloadSchema>;

export const FraudRejectedPayloadSchema = z.object({
  fraud_check_id: z.string().min(1),
  order_id: z.string().min(1),
  risk_score: z.number().min(0).max(100),
  reason: z.string().min(1),
  rejected_at: z.string().datetime().or(z.string()),
});
export type FraudRejectedPayload = z.infer<typeof FraudRejectedPayloadSchema>;
