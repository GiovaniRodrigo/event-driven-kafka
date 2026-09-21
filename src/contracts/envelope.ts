import { z } from 'zod';

/**
 * Standard Event Envelope Schema.
 * Every event in the system must conform to this schema.
 */
export const EventEnvelopeSchema = z.object({
  event_id: z.string().uuid().or(z.string().min(1)),
  event_type: z.string().min(1),
  event_version: z.number().int().positive().default(1),
  aggregate_id: z.string().min(1),
  aggregate_type: z.string().min(1),
  occurred_at: z.string().datetime().or(z.string().regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)),
  producer: z.string().min(1),
  correlation_id: z.string().min(1),
  causation_id: z.string().min(1),
  schema_version: z.number().int().positive().default(1),
  payload: z.record(z.unknown()),
});

export type EventEnvelope<T = Record<string, unknown>> = Omit<z.infer<typeof EventEnvelopeSchema>, 'payload'> & {
  payload: T;
};

/**
 * Helper to construct a strictly typed event envelope.
 */
export function createEventEnvelope<T extends Record<string, unknown>>(params: {
  eventId?: string;
  eventType: string;
  eventVersion?: number;
  aggregateId: string;
  aggregateType: string;
  occurredAt?: string;
  producer: string;
  correlationId: string;
  causationId: string;
  schemaVersion?: number;
  payload: T;
}): EventEnvelope<T> {
  const now = params.occurredAt || new Date().toISOString();
  return {
    event_id: params.eventId || `evt_${crypto.randomUUID()}`,
    event_type: params.eventType,
    event_version: params.eventVersion || 1,
    aggregate_id: params.aggregateId,
    aggregate_type: params.aggregateType,
    occurred_at: now,
    producer: params.producer,
    correlation_id: params.correlationId,
    causation_id: params.causationId,
    schema_version: params.schemaVersion || 1,
    payload: params.payload,
  };
}

/**
 * Validates an unknown object against the envelope and optional payload schema.
 */
export function validateEventEnvelope<T extends Record<string, unknown>>(
  raw: unknown,
  payloadSchema?: z.ZodType<T>
): EventEnvelope<T> {
  const envelope = EventEnvelopeSchema.parse(raw);
  if (payloadSchema) {
    const validatedPayload = payloadSchema.parse(envelope.payload);
    return { ...envelope, payload: validatedPayload };
  }
  return envelope as EventEnvelope<T>;
}
