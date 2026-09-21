import { BaseConsumer } from './base-consumer';
import { DatabaseService } from '../services/database';
import { topics } from '../config';
import { EventTypes } from '../contracts';
import { EventEnvelope } from '../contracts/envelope';
import { ChaosEngine } from '../chaos/chaos-engine';
import { logger } from '../utils/logger';
import { v4 as uuidv4 } from 'uuid';

export class ShippingConsumer extends BaseConsumer {
  private chaosEngine: ChaosEngine;

  constructor(dbInstance?: DatabaseService) {
    super(topics.shipping.name, 'shipping-service-group', 'shipping-service', dbInstance);
    this.chaosEngine = ChaosEngine.getInstance();
  }

  protected async processEvent(envelope: EventEnvelope): Promise<void> {
    const { event_type: eventType, aggregate_id: orderId, correlation_id: correlationId, event_id: eventId } = envelope;

    const shouldFail = this.chaosEngine.shouldFail('shipping');

    if (eventType === EventTypes.ShipmentRequested) {
      if (shouldFail) {
        logger.warn({ event: 'shipping_dispatch_failed', order_id: orderId });
        await this.emit({
          topic: topics.shipping.name,
          eventType: EventTypes.ShipmentFailed,
          aggregateId: orderId,
          aggregateType: 'Shipment',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            reason: 'Carrier API unreachable (Chaos Injection)',
            failed_at: new Date().toISOString(),
          },
        });
        return;
      }

      // Success
      const shipmentId = `ship_${uuidv4().slice(0, 8)}`;
      const trackingNumber = `TRK-${Math.floor(10000000 + Math.random() * 90000000)}`;
      const estimatedDelivery = new Date(Date.now() + 3 * 24 * 60 * 60 * 1000).toISOString();

      await this.emit({
        topic: topics.shipping.name,
        eventType: EventTypes.ShipmentCreated,
        aggregateId: orderId,
        aggregateType: 'Shipment',
        correlationId,
        causationId: eventId,
        payload: {
          shipment_id: shipmentId,
          order_id: orderId,
          tracking_number: trackingNumber,
          carrier: 'FEDEX',
          estimated_delivery: estimatedDelivery,
          created_at: new Date().toISOString(),
        },
      });
    }
  }
}
