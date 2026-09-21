import { BaseConsumer } from './base-consumer';
import { DatabaseService } from '../services/database';
import { topics } from '../config';
import { EventTypes } from '../contracts';
import { EventEnvelope } from '../contracts/envelope';
import { ChaosEngine } from '../chaos/chaos-engine';
import { logger } from '../utils/logger';
import { v4 as uuidv4 } from 'uuid';

export class FraudConsumer extends BaseConsumer {
  private chaosEngine: ChaosEngine;

  constructor(dbInstance?: DatabaseService) {
    super(topics.fraud.name, 'fraud-service-group', 'fraud-service', dbInstance);
    this.chaosEngine = ChaosEngine.getInstance();
  }

  protected async processEvent(envelope: EventEnvelope): Promise<void> {
    const { event_type: eventType, aggregate_id: orderId, correlation_id: correlationId, event_id: eventId } = envelope;
    const payload = envelope.payload as Record<string, any>;

    await this.chaosEngine.checkLatency('fraud');
    const shouldFail = this.chaosEngine.shouldFail('fraud');

    if (eventType === EventTypes.FraudCheckRequested) {
      const fraudCheckId = `fraud_${uuidv4().slice(0, 8)}`;
      const amount = Number(payload.amount) || 0;
      const userId = String(payload.user_id || '');
      const isHighRisk = shouldFail || amount > 10000 || userId.includes('fraudster');

      if (isHighRisk) {
        const riskScore = 92;
        logger.warn({ event: 'fraud_risk_check_rejected', order_id: orderId, riskScore, reason: 'High velocity / risk pattern' });
        await this.emit({
          topic: topics.fraud.name,
          eventType: EventTypes.FraudRejected,
          aggregateId: orderId,
          aggregateType: 'Fraud',
          correlationId,
          causationId: eventId,
          payload: {
            fraud_check_id: fraudCheckId,
            order_id: orderId,
            risk_score: riskScore,
            reason: shouldFail ? 'Chaos forced fraud rejection' : 'Excessive risk score (>75)',
            rejected_at: new Date().toISOString(),
          },
        });
        return;
      }

      // Approved
      const riskScore = Math.floor(Math.random() * 25) + 5;
      await this.emit({
        topic: topics.fraud.name,
        eventType: EventTypes.FraudApproved,
        aggregateId: orderId,
        aggregateType: 'Fraud',
        correlationId,
        causationId: eventId,
        payload: {
          fraud_check_id: fraudCheckId,
          order_id: orderId,
          risk_score: riskScore,
          approved_at: new Date().toISOString(),
        },
      });
    }
  }
}
