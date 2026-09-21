import { DatabaseService } from './database';
import { Order, OrderItem } from '../types';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger';
import { topics } from '../config';
import { EventTypes } from '../contracts';
import { OutboxRelay } from '../infrastructure/outbox/outbox-relay';

export class OrderService {
  constructor(
    private db: DatabaseService,
    private outboxRelay?: OutboxRelay
  ) {}

  async createOrder(
    userId: string,
    items: OrderItem[],
    options: { correlationId?: string; causationId?: string; currency?: string } = {}
  ): Promise<Order> {
    const orderId = `ord_${uuidv4().slice(0, 8)}`;
    const totalAmount = items.reduce((sum, item) => sum + item.price * item.quantity, 0);
    const correlationId = options.correlationId || `corr_${orderId}`;
    const causationId = options.causationId || `cmd_${orderId}`;
    const eventId = `evt_${uuidv4().slice(0, 8)}`;
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

    const orderCreatedPayload = {
      order_id: orderId,
      user_id: userId,
      items,
      total_amount: totalAmount,
      currency: options.currency || 'USD',
      shipping_address: { country: 'US' },
    };

    // ATOMIC TRANSACTION: Both Order Aggregate and Outbox Event are written together
    await this.db.withTransaction(async (client) => {
      await this.db.insertOrder(order, client);

      await this.db.insertOutboxEvent(
        {
          id: eventId,
          aggregateId: orderId,
          aggregateType: 'Order',
          eventType: EventTypes.OrderCreated,
          eventVersion: 1,
          payload: orderCreatedPayload,
          correlationId,
          causationId,
          topic: topics.orders.name,
        },
        client
      );
    });

    logger.info({
      event: 'order_created_transactional',
      order_id: orderId,
      user_id: userId,
      total_amount: totalAmount,
      outbox_event_id: eventId,
      correlation_id: correlationId,
    });

    // Notify the relay to immediately sweep outbox
    if (this.outboxRelay) {
      this.outboxRelay.trigger().catch((err) => {
        logger.warn({ event: 'outbox_trigger_error', error: (err as Error).message });
      });
    }

    return order;
  }
}
