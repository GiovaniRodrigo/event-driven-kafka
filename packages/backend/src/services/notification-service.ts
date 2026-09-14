import { DatabaseService } from './database';
import { logger } from '../utils/logger';

export class NotificationService {
  constructor(_db: DatabaseService) {}

  async sendOrderConfirmation(data: any): Promise<void> {
    try {
      await new Promise((resolve) => setTimeout(resolve, 100));

      logger.info({
        event: 'order_confirmation_sent',
        order_id: data.order_id,
        user_id: data.user_id,
      });
    } catch (error) {
      logger.error({
        event: 'notification_error',
        order_id: data.order_id,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  async sendPaymentConfirmation(data: any): Promise<void> {
    try {
      await new Promise((resolve) => setTimeout(resolve, 100));

      logger.info({
        event: 'payment_confirmation_sent',
        order_id: data.order_id,
        payment_id: data.payment_id,
      });
    } catch (error) {
      logger.error({
        event: 'notification_error',
        order_id: data.order_id,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }
}
