import * as React from 'react';
import { mutate } from 'swr';
import {
  SOCKET_EVENTS,
  orderSummarySchema,
  orderUpdateSchema,
  consumerHealthMapSchema,
  type OrderListResponse,
  type HealthResponse,
} from '@kafka-demo/contracts';
import { getSocket } from '@/lib/realtime';
import { swrKeys } from '@/lib/api';

/**
 * Wires the global Socket.IO broadcast into the SWR cache: the socket only
 * *patches* the cache (`revalidate: false`), so SWR stays the single source of
 * truth and the views re-render without refetching. Mounted once at the app
 * root. Returns the live connection state for a status indicator.
 */
export function useRealtimeSync(): { connected: boolean } {
  const [connected, setConnected] = React.useState(false);

  React.useEffect(() => {
    const socket = getSocket();
    setConnected(socket.connected);

    const onConnect = () => setConnected(true);
    const onDisconnect = () => setConnected(false);

    const onCreated = (raw: unknown) => {
      const parsed = orderSummarySchema.safeParse(raw);
      if (!parsed.success) return;
      const o = parsed.data;
      mutate(
        swrKeys.orders,
        (cur: OrderListResponse | undefined) => {
          const row = {
            order_id: o.order_id,
            user_id: o.user_id,
            status: o.status,
            total_amount: o.total_amount,
            created_at: new Date().toISOString(),
          };
          if (!cur) return { orders: [row] };
          if (cur.orders.some((r) => r.order_id === o.order_id)) return cur;
          return { orders: [row, ...cur.orders] };
        },
        { revalidate: false },
      );
    };

    const onUpdated = (raw: unknown) => {
      const parsed = orderUpdateSchema.safeParse(raw);
      if (!parsed.success) return;
      const u = parsed.data;
      mutate(
        swrKeys.orders,
        (cur: OrderListResponse | undefined) =>
          cur
            ? { orders: cur.orders.map((r) => (r.order_id === u.order_id ? { ...r, status: u.status } : r)) }
            : cur,
        { revalidate: false },
      );
      // Refresh the detail record (if it is being viewed) to pull new fields.
      void mutate(swrKeys.order(u.order_id));
    };

    const onHealth = (raw: unknown) => {
      const parsed = consumerHealthMapSchema.safeParse(raw);
      if (!parsed.success) return;
      mutate(
        swrKeys.health,
        (cur: HealthResponse | undefined) => (cur ? { ...cur, consumers: parsed.data } : cur),
        { revalidate: false },
      );
    };

    socket.on('connect', onConnect);
    socket.on('disconnect', onDisconnect);
    socket.on(SOCKET_EVENTS.orderCreated, onCreated);
    socket.on(SOCKET_EVENTS.orderUpdated, onUpdated);
    socket.on(SOCKET_EVENTS.consumerHealth, onHealth);

    return () => {
      socket.off('connect', onConnect);
      socket.off('disconnect', onDisconnect);
      socket.off(SOCKET_EVENTS.orderCreated, onCreated);
      socket.off(SOCKET_EVENTS.orderUpdated, onUpdated);
      socket.off(SOCKET_EVENTS.consumerHealth, onHealth);
    };
  }, []);

  return { connected };
}
