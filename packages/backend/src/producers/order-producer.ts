import { BaseProducer } from './base-producer';
import { v4 as uuidv4 } from 'uuid';

export class OrderProducer extends BaseProducer {
  constructor() {
    super('orders');
  }

  async produceOrderCreatedEvent(order: any) {
    const eventId = `evt_${uuidv4()}`;
    const correlationId = order.id;

    await this.emit(
      'order.created',
      {
        event_id: eventId,
        order_id: order.id,
        user_id: order.user_id,
        items: order.items,
        total_amount: order.total_amount,
        timestamp: new Date().toISOString(),
        correlation_id: correlationId,
      },
      correlationId
    );
  }
}
