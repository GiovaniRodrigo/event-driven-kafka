import { BaseConsumer } from '../consumers/base-consumer';
import { DatabaseService } from '../services/database';
import { topics } from '../config';
import { EventTypes } from '../contracts';
import { EventEnvelope } from '../contracts/envelope';
import { logger } from '../utils/logger';

export type SagaState =
  | 'CREATED'
  | 'PAYMENT_PENDING'
  | 'PAYMENT_APPROVED'
  | 'INVENTORY_PENDING'
  | 'INVENTORY_RESERVED'
  | 'FRAUD_PENDING'
  | 'FRAUD_APPROVED'
  | 'SHIPPING_PENDING'
  | 'SHIPPED'
  | 'COMPLETED'
  | 'COMPENSATING'
  | 'CANCELLED'
  | 'FAILED';

export interface SagaContext {
  order_id?: string;
  user_id?: string;
  total_amount?: number;
  items?: any[];
  currency?: string;
  shipping_address?: any;
  created_at?: string;
  payment_id?: string;
  payment_authorized_at?: string;
  payment_rejection_reason?: string;
  reservation_id?: string;
  inventory_reserved_at?: string;
  inventory_failure_reason?: string;
  fraud_check_id?: string;
  risk_score?: number;
  fraud_rejection_reason?: string;
  shipment_id?: string;
  tracking_number?: string;
  completed_at?: string;
  shipment_failure_reason?: string;
  compensations_pending?: string[];
  compensations_completed?: string[];
  [key: string]: any;
}

export class SagaOrchestrator extends BaseConsumer {
  constructor(dbInstance?: DatabaseService) {
    super(
      [
        topics.orders.name,
        topics.payments.name,
        topics.inventory.name,
        topics.fraud.name,
        topics.shipping.name,
      ],
      'saga-orchestrator-group',
      'saga-orchestrator',
      dbInstance
    );
  }

  protected async processEvent(envelope: EventEnvelope): Promise<void> {
    const { event_type: eventType, aggregate_id: orderId, correlation_id: correlationId, event_id: eventId } = envelope;
    const payload = envelope.payload as Record<string, any>;

    logger.info({
      event: 'saga_step_received',
      order_id: orderId,
      event_type: eventType,
      correlation_id: correlationId,
      causation_id: eventId,
    });

    const saga = await this.db.getSagaByAggregateId(orderId);
    const context: SagaContext = (saga?.context as SagaContext) || {};

    switch (eventType) {
      // 1. Order Created -> Start Saga -> Request Payment
      case EventTypes.OrderCreated: {
        if (saga && saga.state !== 'CREATED') {
          logger.warn({ event: 'saga_duplicate_order_created_ignored', order_id: orderId, current_state: saga.state });
          return;
        }

        const sagaId = saga?.saga_id || `saga_${orderId}`;
        const newContext: SagaContext = {
          order_id: orderId,
          user_id: payload.user_id,
          total_amount: payload.total_amount,
          items: payload.items,
          currency: payload.currency || 'USD',
          shipping_address: payload.shipping_address || {},
          created_at: envelope.occurred_at,
        };

        await this.db.saveSagaInstance({
          sagaId,
          aggregateId: orderId,
          sagaType: 'ORDER_FULFILLMENT',
          state: 'PAYMENT_PENDING',
          currentStep: 'PAYMENT',
          correlationId,
          context: newContext,
        });

        await this.emit({
          topic: topics.payments.name,
          eventType: EventTypes.PaymentRequested,
          aggregateId: orderId,
          aggregateType: 'Payment',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            user_id: payload.user_id,
            amount: payload.total_amount,
            currency: payload.currency || 'USD',
            payment_method: 'credit_card',
          },
        });
        break;
      }

      // 2. Payment Authorized -> Reserve Inventory
      case EventTypes.PaymentAuthorized: {
        if (!saga || saga.state !== 'PAYMENT_PENDING') {
          logger.warn({ event: 'saga_unexpected_payment_authorized', order_id: orderId, current_state: saga?.state });
          return;
        }

        const updatedContext: SagaContext = {
          ...context,
          payment_id: payload.payment_id,
          payment_authorized_at: payload.authorized_at,
        };

        await this.db.saveSagaInstance({
          sagaId: saga.saga_id,
          aggregateId: orderId,
          sagaType: saga.saga_type,
          state: 'INVENTORY_PENDING',
          currentStep: 'INVENTORY',
          correlationId,
          context: updatedContext,
        });

        await this.emit({
          topic: topics.inventory.name,
          eventType: EventTypes.InventoryReservationRequested,
          aggregateId: orderId,
          aggregateType: 'Inventory',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            items: updatedContext.items || [],
          },
        });
        break;
      }

      // Payment Rejected -> Order Failed (Terminal)
      case EventTypes.PaymentRejected: {
        if (!saga || saga.state !== 'PAYMENT_PENDING') {
          logger.warn({ event: 'saga_unexpected_payment_rejected', order_id: orderId, current_state: saga?.state });
          return;
        }

        await this.db.saveSagaInstance({
          sagaId: saga.saga_id,
          aggregateId: orderId,
          sagaType: saga.saga_type,
          state: 'FAILED',
          currentStep: 'TERMINAL',
          correlationId,
          context: { ...context, payment_rejection_reason: payload.reason },
          failureReason: `Payment declined: ${payload.reason}`,
        });

        await this.emit({
          topic: topics.orders.name,
          eventType: EventTypes.OrderFailed,
          aggregateId: orderId,
          aggregateType: 'Order',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            user_id: (context.user_id as string) || 'unknown',
            reason: `Payment declined: ${payload.reason}`,
            failed_at: new Date().toISOString(),
            failed_step: 'PAYMENT',
            compensation_status: 'NO_COMPENSATION_NEEDED',
          },
        });
        break;
      }

      // 3. Inventory Reserved -> Request Fraud Check
      case EventTypes.InventoryReserved: {
        if (!saga || saga.state !== 'INVENTORY_PENDING') {
          logger.warn({ event: 'saga_unexpected_inventory_reserved', order_id: orderId, current_state: saga?.state });
          return;
        }

        const updatedContext: SagaContext = {
          ...context,
          reservation_id: payload.reservation_id,
          inventory_reserved_at: payload.reserved_at,
        };

        await this.db.saveSagaInstance({
          sagaId: saga.saga_id,
          aggregateId: orderId,
          sagaType: saga.saga_type,
          state: 'FRAUD_PENDING',
          currentStep: 'FRAUD_CHECK',
          correlationId,
          context: updatedContext,
        });

        await this.emit({
          topic: topics.fraud.name,
          eventType: EventTypes.FraudCheckRequested,
          aggregateId: orderId,
          aggregateType: 'Fraud',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            user_id: updatedContext.user_id || 'unknown',
            amount: updatedContext.total_amount || 0,
            payment_id: updatedContext.payment_id || 'pay_unknown',
          },
        });
        break;
      }

      // Inventory Failed -> Compensate (Refund Payment)
      case EventTypes.InventoryReservationFailed: {
        if (!saga || saga.state !== 'INVENTORY_PENDING') {
          logger.warn({ event: 'saga_unexpected_inventory_failed', order_id: orderId, current_state: saga?.state });
          return;
        }

        const updatedContext: SagaContext = {
          ...context,
          inventory_failure_reason: payload.reason,
          compensations_pending: ['PAYMENT_REFUND'],
        };

        await this.db.saveSagaInstance({
          sagaId: saga.saga_id,
          aggregateId: orderId,
          sagaType: saga.saga_type,
          state: 'COMPENSATING',
          currentStep: 'COMPENSATING_PAYMENT',
          correlationId,
          context: updatedContext,
          failureReason: `Inventory out of stock: ${payload.reason}`,
        });

        // Trigger Compensation: Refund Payment
        await this.emit({
          topic: topics.payments.name,
          eventType: EventTypes.PaymentRefundRequested,
          aggregateId: orderId,
          aggregateType: 'Payment',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            payment_id: updatedContext.payment_id,
            amount: updatedContext.total_amount || 0,
            reason: `Inventory reservation failed: ${payload.reason}`,
          },
        });
        break;
      }

      // 4. Fraud Approved -> Request Shipment
      case EventTypes.FraudApproved: {
        if (!saga || saga.state !== 'FRAUD_PENDING') {
          logger.warn({ event: 'saga_unexpected_fraud_approved', order_id: orderId, current_state: saga?.state });
          return;
        }

        const updatedContext: SagaContext = {
          ...context,
          fraud_check_id: payload.fraud_check_id,
          risk_score: payload.risk_score,
        };

        await this.db.saveSagaInstance({
          sagaId: saga.saga_id,
          aggregateId: orderId,
          sagaType: saga.saga_type,
          state: 'SHIPPING_PENDING',
          currentStep: 'SHIPPING',
          correlationId,
          context: updatedContext,
        });

        await this.emit({
          topic: topics.shipping.name,
          eventType: EventTypes.ShipmentRequested,
          aggregateId: orderId,
          aggregateType: 'Shipment',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            user_id: updatedContext.user_id || 'unknown',
            items: updatedContext.items || [],
            shipping_address: updatedContext.shipping_address || {},
          },
        });
        break;
      }

      // Fraud Rejected -> Compensate (Release Inventory + Refund Payment)
      case EventTypes.FraudRejected: {
        if (!saga || saga.state !== 'FRAUD_PENDING') {
          logger.warn({ event: 'saga_unexpected_fraud_rejected', order_id: orderId, current_state: saga?.state });
          return;
        }

        const updatedContext: SagaContext = {
          ...context,
          fraud_rejection_reason: payload.reason,
          compensations_pending: ['INVENTORY_RELEASE', 'PAYMENT_REFUND'],
          compensations_completed: [],
        };

        await this.db.saveSagaInstance({
          sagaId: saga.saga_id,
          aggregateId: orderId,
          sagaType: saga.saga_type,
          state: 'COMPENSATING',
          currentStep: 'COMPENSATING_FRAUD',
          correlationId,
          context: updatedContext,
          failureReason: `Fraud risk check rejected: ${payload.reason}`,
        });

        // Trigger Compensation 1: Release Inventory
        await this.emit({
          topic: topics.inventory.name,
          eventType: EventTypes.InventoryReleased,
          aggregateId: orderId,
          aggregateType: 'Inventory',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            reservation_id: updatedContext.reservation_id,
            items: updatedContext.items || [],
            released_at: new Date().toISOString(),
            reason: `Fraud rejection: ${payload.reason}`,
          },
        });

        // Trigger Compensation 2: Refund Payment
        await this.emit({
          topic: topics.payments.name,
          eventType: EventTypes.PaymentRefundRequested,
          aggregateId: orderId,
          aggregateType: 'Payment',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            payment_id: updatedContext.payment_id,
            amount: updatedContext.total_amount || 0,
            reason: `Fraud rejection: ${payload.reason}`,
          },
        });
        break;
      }

      // 5. Shipment Created -> Order Completed (Terminal Success)
      case EventTypes.ShipmentCreated: {
        if (!saga || saga.state !== 'SHIPPING_PENDING') {
          logger.warn({ event: 'saga_unexpected_shipment_created', order_id: orderId, current_state: saga?.state });
          return;
        }

        const updatedContext: SagaContext = {
          ...context,
          shipment_id: payload.shipment_id,
          tracking_number: payload.tracking_number,
          completed_at: new Date().toISOString(),
        };

        await this.db.saveSagaInstance({
          sagaId: saga.saga_id,
          aggregateId: orderId,
          sagaType: saga.saga_type,
          state: 'COMPLETED',
          currentStep: 'TERMINAL_SUCCESS',
          correlationId,
          context: updatedContext,
        });

        // Emit OrderCompleted
        await this.emit({
          topic: topics.orders.name,
          eventType: EventTypes.OrderCompleted,
          aggregateId: orderId,
          aggregateType: 'Order',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            user_id: updatedContext.user_id || 'unknown',
            completed_at: updatedContext.completed_at || new Date().toISOString(),
            total_amount: updatedContext.total_amount || 0,
            payment_id: updatedContext.payment_id,
            shipment_id: payload.shipment_id,
            tracking_number: payload.tracking_number,
          },
        });

        // Request Customer Notification
        await this.emit({
          topic: topics.notifications.name,
          eventType: EventTypes.NotificationRequested,
          aggregateId: orderId,
          aggregateType: 'Notification',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            user_id: updatedContext.user_id || 'unknown',
            channel: 'EMAIL',
            template: 'ORDER_CONFIRMATION_SHIPPED',
            recipient: `${updatedContext.user_id || 'customer'}@example.com`,
            data: {
              tracking_number: payload.tracking_number,
              total_amount: updatedContext.total_amount,
            },
          },
        });
        break;
      }

      // Shipment Failed -> Compensate (Release Inventory + Refund Payment)
      case EventTypes.ShipmentFailed: {
        if (!saga || saga.state !== 'SHIPPING_PENDING') {
          logger.warn({ event: 'saga_unexpected_shipment_failed', order_id: orderId, current_state: saga?.state });
          return;
        }

        const updatedContext: SagaContext = {
          ...context,
          shipment_failure_reason: payload.reason,
          compensations_pending: ['INVENTORY_RELEASE', 'PAYMENT_REFUND'],
        };

        await this.db.saveSagaInstance({
          sagaId: saga.saga_id,
          aggregateId: orderId,
          sagaType: saga.saga_type,
          state: 'COMPENSATING',
          currentStep: 'COMPENSATING_SHIPPING',
          correlationId,
          context: updatedContext,
          failureReason: `Shipment dispatch failed: ${payload.reason}`,
        });

        await this.emit({
          topic: topics.inventory.name,
          eventType: EventTypes.InventoryReleased,
          aggregateId: orderId,
          aggregateType: 'Inventory',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            reservation_id: updatedContext.reservation_id,
            items: updatedContext.items || [],
            released_at: new Date().toISOString(),
            reason: `Shipment failed: ${payload.reason}`,
          },
        });

        await this.emit({
          topic: topics.payments.name,
          eventType: EventTypes.PaymentRefundRequested,
          aggregateId: orderId,
          aggregateType: 'Payment',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            payment_id: updatedContext.payment_id,
            amount: updatedContext.total_amount || 0,
            reason: `Shipment failed: ${payload.reason}`,
          },
        });
        break;
      }

      // Compensation Resolvers
      case EventTypes.PaymentRefunded: {
        if (!saga || saga.state !== 'COMPENSATING') {
          logger.warn({ event: 'saga_unexpected_payment_refunded', order_id: orderId, current_state: saga?.state });
          return;
        }

        const compensations: string[] = Array.isArray(context.compensations_completed)
          ? [...context.compensations_completed]
          : [];
        compensations.push('PAYMENT_REFUND');
        const updatedContext: SagaContext = { ...context, compensations_completed: compensations };

        await this.db.saveSagaInstance({
          sagaId: saga.saga_id,
          aggregateId: orderId,
          sagaType: saga.saga_type,
          state: 'CANCELLED',
          currentStep: 'TERMINAL_CANCELLED',
          correlationId,
          context: updatedContext,
        });

        // Emit OrderCancelled
        await this.emit({
          topic: topics.orders.name,
          eventType: EventTypes.OrderCancelled,
          aggregateId: orderId,
          aggregateType: 'Order',
          correlationId,
          causationId: eventId,
          payload: {
            order_id: orderId,
            user_id: (context.user_id as string) || 'unknown',
            reason: saga.failure_reason || 'Order cancelled after compensation',
            cancelled_at: new Date().toISOString(),
            compensated_steps: compensations,
          },
        });
        break;
      }

      default:
        break;
    }
  }
}
