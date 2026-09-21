import { BaseConsumer } from './base-consumer';
import { DatabaseService } from '../services/database';
import { topics } from '../config';
import { EventTypes } from '../contracts';
import { EventEnvelope } from '../contracts/envelope';
import { ChaosEngine } from '../chaos/chaos-engine';
import { logger } from '../utils/logger';
import { v4 as uuidv4 } from 'uuid';

export class PaymentConsumer extends BaseConsumer {
  private chaosEngine: ChaosEngine;

  constructor(dbInstance?: DatabaseService) {
    super(topics.payments.name, 'payment-service-group', 'payment-service', dbInstance);
    this.chaosEngine = ChaosEngine.getInstance();
  }

  protected async processEvent(envelope: EventEnvelope): Promise<void> {
    const { event_type: eventType, aggregate_id: orderId, correlation_id: correlationId, event_id: eventId } = envelope;
    const payload = envelope.payload as Record<string, any>;

    // 1. Check Chaos Fault Injection
    await this.chaosEngine.checkLatency('payment');
    const shouldFail = this.chaosEngine.shouldFail('payment');

    if (eventType === EventTypes.PaymentRequested) {
      if (shouldFail) {
        logger.warn({ event: 'chaos_payment_failure_triggered', order_id: orderId });
        await this.emit({
          topic: topics.payments.name,
          eventType: EventTypes.PaymentRejected,
          aggregateId: orderId,
          aggregateType: 'Payment',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            user_id: payload.user_id,
            amount: payload.amount,
            reason: 'Card authorization failed (Chaos Injection)',
            rejected_at: new Date().toISOString(),
          },
        });
        return;
      }

      // Happy path authorization
      const paymentId = `pay_${uuidv4().slice(0, 8)}`;
      const authCode = `AUTH_${Math.floor(100000 + Math.random() * 900000)}`;
      const now = new Date().toISOString();

      await this.emit({
        topic: topics.payments.name,
        eventType: EventTypes.PaymentAuthorized,
        aggregateId: orderId,
        aggregateType: 'Payment',
        correlationId,
        causationId: eventId,
        payload: {
          payment_id: paymentId,
          order_id: orderId,
          user_id: payload.user_id,
          amount: payload.amount,
          authorization_code: authCode,
          authorized_at: now,
        },
      });
      return;
    }

    if (eventType === EventTypes.PaymentRefundRequested) {
      const refundId = `ref_${uuidv4().slice(0, 8)}`;
      const now = new Date().toISOString();

      logger.info({ event: 'payment_refund_executed', order_id: orderId, payment_id: payload.payment_id });

      await this.emit({
        topic: topics.payments.name,
        eventType: EventTypes.PaymentRefunded,
        aggregateId: orderId,
        aggregateType: 'Payment',
        correlationId,
        causationId: eventId,
        payload: {
          refund_id: refundId,
          payment_id: payload.payment_id || `pay_unknown`,
          order_id: orderId,
          amount: payload.amount,
          refunded_at: now,
        },
      });
    }
  }
}
