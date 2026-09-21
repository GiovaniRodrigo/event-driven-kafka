import { BaseConsumer } from '../../consumers/base-consumer';
import { DatabaseService } from '../../services/database';
import { topics } from '../../config';
import { EventTypes } from '../../contracts';
import { EventEnvelope } from '../../contracts/envelope';
import { RealtimeGateway } from '../../realtime/realtime-gateway';
import { logger } from '../../utils/logger';
import { PoolClient } from 'pg';

export class ProjectionConsumer extends BaseConsumer {
  constructor(
    private gateway: RealtimeGateway,
    dbInstance?: DatabaseService
  ) {
    super(
      [
        topics.orders.name,
        topics.payments.name,
        topics.inventory.name,
        topics.fraud.name,
        topics.shipping.name,
        topics.notifications.name,
      ],
      'projection-read-model-group',
      'projection-consumer',
      dbInstance
    );
  }

  protected async processEvent(envelope: EventEnvelope): Promise<void> {
    const { event_type: eventType, aggregate_id: orderId } = envelope;

    // 1. Persist to Immutable Event Store (Audit Log / Sourcing)
    await this.db.appendToEventStore(envelope);

    // 2. Also keep legacy order_events updated for timeline
    await this.db.recordEvent(orderId, eventType, envelope.producer || 'system');

    // 3. Update Materialized CQRS Read Models idempotently (DB mutations only)
    await this.applyHistoricalEvent(envelope);

    // 4. Emit live realtime telemetry notifications (Live stream only, bypassed during replay)
    this.emitLiveNotifications(envelope);
  }

  /**
   * Applies an event to the CQRS read models without emitting external side effects.
   * Safe to call during Event Replay or live stream processing.
   * STRICT GUARANTEE: Zero Kafka emissions, Zero websocket broadcasts, Zero command side-effects.
   */
  public async applyHistoricalEvent(envelope: EventEnvelope, externalClient?: PoolClient): Promise<void> {
    const { event_type: eventType, aggregate_id: orderId, occurred_at: occurredAt, event_id: eventId } = envelope;
    const payload = envelope.payload as Record<string, any>;

    const executeInTx = async (client: PoolClient) => {
      // Check projection-level idempotency to prevent duplicate mutations on replay
      const alreadyApplied = await this.db.isProjectionEventApplied('order-fulfillment-projection', eventId, client);
      if (alreadyApplied) {
        logger.info({
          event: 'projection_duplicate_event_skipped',
          event_id: eventId,
          order_id: orderId,
          event_type: eventType,
        });
        return;
      }

      switch (eventType) {
        case EventTypes.OrderCreated: {
          await client.query(
            `
            INSERT INTO order_read_model (
              order_id, user_id, status, total_amount, currency, items, created_at, updated_at, version
            ) VALUES ($1, $2, 'pending', $3, $4, $5, $6, $6, 1)
            ON CONFLICT (order_id) DO UPDATE
            SET status = 'pending', total_amount = EXCLUDED.total_amount, items = EXCLUDED.items, updated_at = EXCLUDED.updated_at
          `,
            [
              orderId,
              payload.user_id,
              payload.total_amount,
              payload.currency || 'USD',
              JSON.stringify(payload.items || []),
              occurredAt,
            ]
          );
          break;
        }

        case EventTypes.PaymentRequested: {
          await client.query(
            `
            UPDATE order_read_model SET status = 'payment_pending', updated_at = $2 WHERE order_id = $1
          `,
            [orderId, occurredAt]
          );
          break;
        }

        case EventTypes.PaymentAuthorized: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'payment_approved', payment_id = $2, updated_at = $3
            WHERE order_id = $1
          `,
            [orderId, payload.payment_id, occurredAt]
          );

          await client.query(
            `
            INSERT INTO payment_read_model (
              payment_id, order_id, user_id, amount, status, authorization_code, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, 'AUTHORIZED', $5, $6, $6)
            ON CONFLICT (payment_id) DO UPDATE SET status = 'AUTHORIZED', updated_at = EXCLUDED.updated_at
          `,
            [payload.payment_id, orderId, payload.user_id, payload.amount, payload.authorization_code, occurredAt]
          );
          break;
        }

        case EventTypes.PaymentRejected: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'payment_rejected', failure_reason = $2, updated_at = $3
            WHERE order_id = $1
          `,
            [orderId, payload.reason, occurredAt]
          );
          break;
        }

        case EventTypes.PaymentRefunded: {
          await client.query(
            `
            UPDATE payment_read_model
            SET status = 'REFUNDED', refund_id = $2, updated_at = $3
            WHERE order_id = $1
          `,
            [orderId, payload.refund_id || `ref_${orderId}`, occurredAt]
          );
          break;
        }

        case EventTypes.InventoryReservationRequested: {
          await client.query(
            `
            UPDATE order_read_model SET status = 'inventory_pending', updated_at = $2 WHERE order_id = $1
          `,
            [orderId, occurredAt]
          );
          break;
        }

        case EventTypes.InventoryReserved: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'inventory_reserved', reservation_id = $2, updated_at = $3
            WHERE order_id = $1
          `,
            [orderId, payload.reservation_id, occurredAt]
          );

          if (Array.isArray(payload.items)) {
            for (const item of payload.items) {
              await client.query(
                `
                UPDATE inventory_read_model
                SET reserved_stock = reserved_stock + $1,
                    available_stock = GREATEST(0, available_stock - $1),
                    updated_at = $2
                WHERE sku = $3
              `,
                [item.quantity || 1, occurredAt, item.sku]
              );
            }
          }
          break;
        }

        case EventTypes.InventoryReleased: {
          if (Array.isArray(payload.items)) {
            for (const item of payload.items) {
              await client.query(
                `
                UPDATE inventory_read_model
                SET reserved_stock = GREATEST(0, reserved_stock - $1),
                    available_stock = available_stock + $1,
                    updated_at = $2
                WHERE sku = $3
              `,
                [item.quantity || 1, occurredAt, item.sku]
              );
            }
          }
          break;
        }

        case EventTypes.InventoryReservationFailed: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'inventory_failed', failure_reason = $2, updated_at = $3
            WHERE order_id = $1
          `,
            [orderId, payload.reason, occurredAt]
          );
          break;
        }

        case EventTypes.FraudCheckRequested: {
          await client.query(
            `
            UPDATE order_read_model SET status = 'fraud_pending', updated_at = $2 WHERE order_id = $1
          `,
            [orderId, occurredAt]
          );
          break;
        }

        case EventTypes.FraudApproved: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'fraud_approved', fraud_check_id = $2, updated_at = $3
            WHERE order_id = $1
          `,
            [orderId, payload.fraud_check_id, occurredAt]
          );
          break;
        }

        case EventTypes.FraudRejected: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'fraud_rejected', failure_reason = $2, updated_at = $3
            WHERE order_id = $1
          `,
            [orderId, payload.reason, occurredAt]
          );
          break;
        }

        case EventTypes.ShipmentRequested: {
          await client.query(
            `
            UPDATE order_read_model SET status = 'shipping_pending', updated_at = $2 WHERE order_id = $1
          `,
            [orderId, occurredAt]
          );
          break;
        }

        case EventTypes.ShipmentCreated: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'shipping_created', shipment_id = $2, tracking_number = $3, updated_at = $4
            WHERE order_id = $1
          `,
            [orderId, payload.shipment_id, payload.tracking_number, occurredAt]
          );

          await client.query(
            `
            INSERT INTO shipment_read_model (
              shipment_id, order_id, tracking_number, carrier, status, estimated_delivery, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, 'CREATED', $5, $6, $6)
            ON CONFLICT (shipment_id) DO UPDATE SET status = 'CREATED', updated_at = EXCLUDED.updated_at
          `,
            [payload.shipment_id, orderId, payload.tracking_number, payload.carrier || 'FEDEX', payload.estimated_delivery, occurredAt]
          );
          break;
        }

        case EventTypes.OrderCompleted: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'completed', updated_at = $2
            WHERE order_id = $1
          `,
            [orderId, occurredAt]
          );
          break;
        }

        case EventTypes.OrderCancelled: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'cancelled', failure_reason = $2, updated_at = $3
            WHERE order_id = $1
          `,
            [orderId, payload.reason, occurredAt]
          );
          break;
        }

        case EventTypes.OrderFailed: {
          await client.query(
            `
            UPDATE order_read_model
            SET status = 'failed', failure_reason = $2, updated_at = $3
            WHERE order_id = $1
          `,
            [orderId, payload.reason, occurredAt]
          );
          break;
        }
      }

      // Record in projection_applied_events within same atomic transaction
      await this.db.markProjectionEventApplied('order-fulfillment-projection', eventId, orderId, client);
    };

    if (externalClient) {
      await executeInTx(externalClient);
    } else {
      const client = await this.db.getPool().connect();
      try {
        await client.query('BEGIN');
        await executeInTx(client);
        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK');
        logger.error({
          event: 'projection_error',
          order_id: orderId,
          event_type: eventType,
          error: (err as Error).message,
        });
        throw err;
      } finally {
        client.release();
      }
    }
  }

  /**
   * Dispatches live websocket notifications to connected clients.
   * Isolated from projection persistence and skipped during historical event replays.
   */
  public emitLiveNotifications(envelope: EventEnvelope): void {
    if (!this.gateway) return;

    const { event_type: eventType, aggregate_id: orderId, correlation_id: correlationId, occurred_at: occurredAt } = envelope;
    const payload = envelope.payload as Record<string, any>;

    this.gateway.orderEvent(orderId, {
      event_type: eventType,
      topic: envelope.producer || 'unknown',
      timestamp: occurredAt,
      correlation_id: correlationId,
      causation_id: envelope.causation_id,
      payload,
    });

    switch (eventType) {
      case EventTypes.OrderCreated:
        this.gateway.orderCreated({
          order_id: orderId,
          user_id: payload.user_id,
          status: 'pending',
          total_amount: payload.total_amount,
          created_at: occurredAt,
        });
        break;
      case EventTypes.PaymentRequested:
        this.gateway.orderUpdated({ order_id: orderId, status: 'payment_pending', step: 'PAYMENT' });
        break;
      case EventTypes.PaymentAuthorized:
        this.gateway.orderUpdated({ order_id: orderId, status: 'payment_approved', step: 'PAYMENT' });
        break;
      case EventTypes.PaymentRejected:
        this.gateway.orderUpdated({ order_id: orderId, status: 'payment_rejected', step: 'PAYMENT' });
        break;
      case EventTypes.InventoryReservationRequested:
        this.gateway.orderUpdated({ order_id: orderId, status: 'inventory_pending', step: 'INVENTORY' });
        break;
      case EventTypes.InventoryReserved:
        this.gateway.orderUpdated({ order_id: orderId, status: 'inventory_reserved', step: 'INVENTORY' });
        break;
      case EventTypes.InventoryReservationFailed:
        this.gateway.orderUpdated({ order_id: orderId, status: 'inventory_failed', step: 'INVENTORY' });
        break;
      case EventTypes.FraudCheckRequested:
        this.gateway.orderUpdated({ order_id: orderId, status: 'fraud_pending', step: 'FRAUD' });
        break;
      case EventTypes.FraudApproved:
        this.gateway.orderUpdated({ order_id: orderId, status: 'fraud_approved', step: 'FRAUD' });
        break;
      case EventTypes.FraudRejected:
        this.gateway.orderUpdated({ order_id: orderId, status: 'fraud_rejected', step: 'FRAUD' });
        break;
      case EventTypes.ShipmentRequested:
        this.gateway.orderUpdated({ order_id: orderId, status: 'shipping_pending', step: 'SHIPPING' });
        break;
      case EventTypes.ShipmentCreated:
        this.gateway.orderUpdated({ order_id: orderId, status: 'shipping_created', step: 'SHIPPING' });
        break;
      case EventTypes.OrderCompleted:
        this.gateway.orderUpdated({ order_id: orderId, status: 'completed' });
        break;
      case EventTypes.OrderCancelled:
        this.gateway.orderUpdated({ order_id: orderId, status: 'cancelled' });
        break;
      case EventTypes.OrderFailed:
        this.gateway.orderUpdated({ order_id: orderId, status: 'failed' });
        break;
    }
  }
}
