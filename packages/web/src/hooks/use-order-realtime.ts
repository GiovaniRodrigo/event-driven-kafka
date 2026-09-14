import * as React from 'react';
import { mutate } from 'swr';
import { SOCKET_EVENTS, orderEventPayloadSchema, type OrderDetail } from '@kafka-demo/contracts';
import { getSocket } from '@/lib/realtime';
import { swrKeys } from '@/lib/api';

/**
 * Subscribes to a single order's room while the detail view is mounted and
 * appends each incoming `order:event` to that order's cached timeline live,
 * leaving the room on unmount. The server payload carries an extra `order_id`
 * which the event schema strips on parse.
 */
export function useOrderRealtime(id: string | undefined): void {
  React.useEffect(() => {
    if (!id) return;
    const socket = getSocket();
    socket.emit('subscribe', id);

    const onEvent = (raw: unknown) => {
      if (!raw || typeof raw !== 'object' || (raw as { order_id?: string }).order_id !== id) return;
      const parsed = orderEventPayloadSchema.safeParse(raw);
      if (!parsed.success) return;
      const event = parsed.data;
      mutate(
        swrKeys.order(id),
        (cur: OrderDetail | undefined) => {
          if (!cur) return cur;
          const exists = cur.events.some(
            (e) => e.event_type === event.event_type && e.timestamp === event.timestamp,
          );
          return exists ? cur : { ...cur, events: [...cur.events, event] };
        },
        { revalidate: false },
      );
    };

    socket.on(SOCKET_EVENTS.orderEvent, onEvent);
    return () => {
      socket.emit('unsubscribe', id);
      socket.off(SOCKET_EVENTS.orderEvent, onEvent);
    };
  }, [id]);
}
