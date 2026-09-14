import { BaseConsumer } from './base-consumer';
import { InventoryService } from '../services/inventory-service';
import { logger } from '../utils/logger';
import { v4 as uuidv4 } from 'uuid';

/**
 * Consumes `payment.approved` from the `payments` topic, reserves inventory,
 * and emits `inventory.reserved` to the `inventory` topic for the
 * notification stage.
 */
export class InventoryConsumer extends BaseConsumer {
  private inventoryService: InventoryService;

  constructor() {
    super('payments', 'inventory-processor-group');
    this.inventoryService = new InventoryService(this.db);
  }

  protected async processEvent(event: any): Promise<void> {
    const startTime = Date.now();

    if (event.type !== 'payment.approved') {
      return;
    }

    logger.info({
      event: 'inventory_processing_start',
      order_id: event.order_id,
      event_id: event.event_id,
    });

    const result = await this.inventoryService.reserveInventory({
      order_id: event.order_id,
      items: event.items,
    });

    await this.db.updateOrderStatus(event.order_id, 'inventory_reserved', {
      reservation_id: result.reservation_id,
    });
    await this.db.recordEvent(event.order_id, 'inventory.reserved', 'inventory');

    await this.emit(
      'inventory',
      'inventory.reserved',
      {
        event_id: `evt_${uuidv4().slice(0, 8)}`,
        order_id: event.order_id,
        user_id: event.user_id,
        items: event.items,
        reservation_id: result.reservation_id,
        timestamp: new Date().toISOString(),
        correlation_id: event.correlation_id || event.order_id,
      },
      event.order_id
    );

    const duration = Date.now() - startTime;
    logger.info({
      event: 'inventory_reserved',
      order_id: event.order_id,
      reservation_id: result.reservation_id,
      duration_ms: duration,
    });
  }
}
