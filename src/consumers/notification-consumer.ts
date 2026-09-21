import { BaseConsumer } from './base-consumer';
import { DatabaseService } from '../services/database';
import { topics } from '../config';
import { EventTypes } from '../contracts';
import { EventEnvelope } from '../contracts/envelope';
import { ChaosEngine } from '../chaos/chaos-engine';
import { logger } from '../utils/logger';
import { v4 as uuidv4 } from 'uuid';

export class NotificationConsumer extends BaseConsumer {
  private chaosEngine: ChaosEngine;

  constructor(dbInstance?: DatabaseService) {
    super(topics.notifications.name, 'notification-service-group', 'notification-service', dbInstance);
    this.chaosEngine = ChaosEngine.getInstance();
  }

  protected async processEvent(envelope: EventEnvelope): Promise<void> {
    const { event_type: eventType, aggregate_id: orderId, correlation_id: correlationId, event_id: eventId } = envelope;
    const payload = envelope.payload as Record<string, any>;

    const shouldFail = this.chaosEngine.shouldFail('notification');

    if (eventType === EventTypes.NotificationRequested) {
      if (shouldFail) {
        throw new Error('Notification mail server unreachable (Chaos Injection)');
      }

      const notificationId = `notif_${uuidv4().slice(0, 8)}`;
      const now = new Date().toISOString();

      logger.info({
        event: 'notification_delivered',
        order_id: orderId,
        recipient: payload.recipient,
        channel: payload.channel,
        template: payload.template,
      });

      await this.emit({
        topic: topics.notifications.name,
        eventType: EventTypes.NotificationSent,
        aggregateId: orderId,
        aggregateType: 'Notification',
        correlationId,
        causationId: eventId,
        payload: {
          notification_id: notificationId,
          order_id: orderId,
          user_id: payload.user_id || 'unknown',
          channel: payload.channel || 'EMAIL',
          sent_at: now,
        },
      });
    }
  }
}
