import { DatabaseService } from './database';
import { OrderProducer } from '../producers/order-producer';
import { Order, OrderItem } from '../types';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger';

export class OrderService {
  private producer: OrderProducer;

  constructor(producer: OrderProducer, private db: DatabaseService) {
    this.producer = producer;
  }

  async createOrder(userId: string, items: OrderItem[]): Promise<Order> {
    const orderId = `ord_${uuidv4().slice(0, 8)}`;
    const totalAmount = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

    const now = new Date();

    const order: Order = {
      id: orderId,
      user_id: userId,
      items,
      status: 'pending',
      total_amount: totalAmount,
      created_at: now,
      updated_at: now,
    };

    await this.db.insertOrder(order);
    await this.db.recordEvent(orderId, 'order.created', 'orders');

    logger.info({
      event: 'order_created',
      order_id: orderId,
      user_id: userId,
      total_amount: totalAmount,
      items_count: items.length,
    });

    try {
      const eventId = `evt_${uuidv4().slice(0, 8)}`;
      await this.producer.emit(
        'order.created',
        {
          event_id: eventId,
          order_id: orderId,
          user_id: userId,
          items,
          total_amount: totalAmount,
          timestamp: now.toISOString(),
          correlation_id: orderId,
        },
        orderId
      );

      logger.info({
        event: 'order_event_emitted',
        order_id: orderId,
        event_id: eventId,
      });
    } catch (error) {
      logger.error({
        event: 'order_event_emit_error',
        order_id: orderId,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }

    return order;
  }
}
