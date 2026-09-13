import { BaseConsumer } from './base-consumer';
import { NotificationService } from '../services/notification-service';
import { logger } from '../utils/logger';

/**
 * Consumes `inventory.reserved` from the `inventory` topic, sends the order
 * confirmation, and marks the order `completed`. This is the final stage of
 * the pipeline.
 */
export class NotificationConsumer extends BaseConsumer {
  private notificationService: NotificationService;

  constructor() {
    super('inventory', 'notification-processor-group');
    this.notificationService = new NotificationService(this.db);
  }

  protected async processEvent(event: any): Promise<void> {
    const startTime = Date.now();

    if (event.type !== 'inventory.reserved') {
      return;
    }

    logger.info({
      event: 'notification_sending_start',
      order_id: event.order_id,
      user_id: event.user_id,
    });

    await this.notificationService.sendOrderConfirmation({
      order_id: event.order_id,
      user_id: event.user_id,
      items: event.items,
    });

    await this.db.updateOrderStatus(event.order_id, 'completed', {});
    await this.db.recordEvent(event.order_id, 'notification.sent', 'notifications');

    const duration = Date.now() - startTime;
    logger.info({
      event: 'notification_sent',
      order_id: event.order_id,
      duration_ms: duration,
    });
  }
}
