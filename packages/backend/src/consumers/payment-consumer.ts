import { BaseConsumer } from './base-consumer';
import { PaymentService } from '../services/payment-service';
import { logger } from '../utils/logger';
import { v4 as uuidv4 } from 'uuid';

/**
 * Consumes `order.created` from the `orders` topic, processes payment, and
 * emits `payment.approved` to the `payments` topic for the inventory stage.
 */
export class PaymentConsumer extends BaseConsumer {
  private paymentService: PaymentService;

  constructor() {
    super('orders', 'payment-processor-group');
    this.paymentService = new PaymentService(this.db);
  }

  protected async processEvent(event: any): Promise<void> {
    const startTime = Date.now();

    if (event.type !== 'order.created') {
      return;
    }

    logger.info({
      event: 'payment_processing_start',
      order_id: event.order_id,
      event_id: event.event_id,
    });

    const result = await this.paymentService.processPayment({
      order_id: event.order_id,
      user_id: event.user_id,
      amount: event.total_amount,
      items: event.items,
    });

    await this.db.updateOrderStatus(event.order_id, 'payment_approved', {
      payment_id: result.payment_id,
    });
    await this.db.recordEvent(event.order_id, 'payment.approved', 'payments');

    await this.emit(
      'payments',
      'payment.approved',
      {
        event_id: `evt_${uuidv4().slice(0, 8)}`,
        order_id: event.order_id,
        user_id: event.user_id,
        items: event.items,
        amount: event.total_amount,
        payment_id: result.payment_id,
        timestamp: new Date().toISOString(),
        correlation_id: event.correlation_id || event.order_id,
      },
      event.order_id
    );

    const duration = Date.now() - startTime;
    logger.info({
      event: 'payment_processed',
      order_id: event.order_id,
      payment_id: result.payment_id,
      duration_ms: duration,
    });
  }
}
