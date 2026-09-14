import useSWR from 'swr';
import { api, swrKeys } from '@/lib/api';

/** Recent orders (list view / overview table). */
export function useOrders() {
  return useSWR(swrKeys.orders, api.listOrders);
}

/** A single order with its event history (detail view). */
export function useOrder(id: string | undefined) {
  return useSWR(id ? swrKeys.order(id) : null, () => api.getOrder(id as string));
}

/** Service + per-consumer health. */
export function useHealth() {
  return useSWR(swrKeys.health, api.getHealth, { refreshInterval: 15000 });
}

/** Aggregate metrics for the KPI row. */
export function useMetrics() {
  return useSWR(swrKeys.metrics, api.getMetrics, { refreshInterval: 15000 });
}
