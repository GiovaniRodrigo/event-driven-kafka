import { z } from 'zod';

export const NotificationRequestedPayloadSchema = z.object({
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  channel: z.enum(['EMAIL', 'SMS', 'PUSH', 'WEBHOOK']).default('EMAIL'),
  template: z.string().min(1),
  recipient: z.string().min(1),
  data: z.record(z.unknown()),
});
export type NotificationRequestedPayload = z.infer<typeof NotificationRequestedPayloadSchema>;

export const NotificationSentPayloadSchema = z.object({
  notification_id: z.string().min(1),
  order_id: z.string().min(1),
  user_id: z.string().min(1),
  channel: z.string().min(1),
  sent_at: z.string().datetime().or(z.string()),
});
export type NotificationSentPayload = z.infer<typeof NotificationSentPayloadSchema>;
