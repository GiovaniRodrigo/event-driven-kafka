import type { OrderStatus, ConsumerHealth } from '@kafka-demo/contracts';

export type SemanticTone = 'neutral' | 'info' | 'primary' | 'success' | 'warning' | 'danger';

/** Human label + semantic tone for each order status the backend can emit. */
export const ORDER_STATUS_META: Record<OrderStatus, { label: string; tone: SemanticTone }> = {
  pending: { label: 'Pending', tone: 'neutral' },
  payment_processing: { label: 'Processing payment', tone: 'info' },
  payment_approved: { label: 'Payment approved', tone: 'primary' },
  inventory_reserved: { label: 'Inventory reserved', tone: 'primary' },
  completed: { label: 'Completed', tone: 'success' },
  failed: { label: 'Failed', tone: 'danger' },
};

/** Ordered pipeline stages, used to render an order's progress. */
export const ORDER_PIPELINE: OrderStatus[] = [
  'pending',
  'payment_processing',
  'payment_approved',
  'inventory_reserved',
  'completed',
];

export const CONSUMER_HEALTH_META: Record<ConsumerHealth, { label: string; tone: SemanticTone }> = {
  healthy: { label: 'Healthy', tone: 'success' },
  degraded: { label: 'Degraded', tone: 'warning' },
  down: { label: 'Down', tone: 'danger' },
};

export function isFailedStatus(status: OrderStatus): boolean {
  return status === 'failed';
}
