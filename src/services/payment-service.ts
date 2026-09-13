import { DatabaseService } from './database';
import { logger } from '../utils/logger';

export class PaymentService {
  constructor(_db: DatabaseService) {}

  async processPayment(paymentData: any): Promise<any> {
    try {
      const processingTime = Math.random() * 2000 + 1000;
      await new Promise((resolve) => setTimeout(resolve, processingTime));

      const paymentId = `pay_${Date.now()}`;

      logger.info({
        event: 'payment_processed',
        order_id: paymentData.order_id,
        payment_id: paymentId,
        amount: paymentData.amount,
      });

      return { payment_id: paymentId, status: 'approved' };
    } catch (error) {
      logger.error({
        event: 'payment_error',
        order_id: paymentData.order_id,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }
}
