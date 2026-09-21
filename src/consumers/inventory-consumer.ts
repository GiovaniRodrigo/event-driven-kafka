import { BaseConsumer } from './base-consumer';
import { DatabaseService } from '../services/database';
import { topics } from '../config';
import { EventTypes } from '../contracts';
import { EventEnvelope } from '../contracts/envelope';
import { ChaosEngine } from '../chaos/chaos-engine';
import { logger } from '../utils/logger';
import { v4 as uuidv4 } from 'uuid';

export class InventoryConsumer extends BaseConsumer {
  private chaosEngine: ChaosEngine;

  constructor(dbInstance?: DatabaseService) {
    super(topics.inventory.name, 'inventory-service-group', 'inventory-service', dbInstance);
    this.chaosEngine = ChaosEngine.getInstance();
  }

  protected async processEvent(envelope: EventEnvelope): Promise<void> {
    const { event_type: eventType, aggregate_id: orderId, correlation_id: correlationId, event_id: eventId } = envelope;
    const payload = envelope.payload as Record<string, any>;

    await this.chaosEngine.checkLatency('inventory');
    const shouldFail = this.chaosEngine.shouldFail('inventory');

    if (eventType === EventTypes.InventoryReservationRequested) {
      const items = (payload.items || []) as Array<{ sku: string; quantity: number; name: string }>;
      const hasOutOfStock = items.some((i) => i.sku === 'OUT_OF_STOCK_ITEM' || i.quantity > 500);

      if (shouldFail || hasOutOfStock) {
        logger.warn({ event: 'inventory_reservation_failed', order_id: orderId, items });
        await this.emit({
          topic: topics.inventory.name,
          eventType: EventTypes.InventoryReservationFailed,
          aggregateId: orderId,
          aggregateType: 'Inventory',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            items,
            reason: shouldFail ? 'Chaos forced inventory failure' : 'SKU out of stock',
            failed_at: new Date().toISOString(),
          },
        });
        return;
      }

      // Successful reservation
      const reservationId = `res_${uuidv4().slice(0, 8)}`;
      const now = new Date().toISOString();

      await this.emit({
        topic: topics.inventory.name,
        eventType: EventTypes.InventoryReserved,
        aggregateId: orderId,
        aggregateType: 'Inventory',
        correlationId,
        causationId: eventId,
        payload: {
          reservation_id: reservationId,
          order_id: orderId,
          items,
          reserved_at: now,
        },
      });
      return;
    }

    if (eventType === EventTypes.InventoryReleased) {
      logger.info({
        event: 'inventory_released_compensation',
        order_id: orderId,
        reservation_id: payload.reservation_id,
        items: payload.items,
        reason: payload.reason,
      });
    }
  }
}
