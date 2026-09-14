import { Server, Socket } from 'socket.io';
import {
  OrderSummary,
  OrderUpdate,
  OrderEventPayload,
  ConsumerHealthMap,
  SOCKET_EVENTS,
} from '@kafka-demo/contracts';
import { logger } from '../utils/logger';

/**
 * Abstraction over the real-time transport so the read-side bridge
 * (RealtimeConsumer) can be unit-tested against a mock instead of a live
 * Socket.IO server.
 */
export interface RealtimeGateway {
  orderCreated(order: OrderSummary): void;
  orderUpdated(update: OrderUpdate): void;
  orderEvent(orderId: string, event: OrderEventPayload): void;
  consumerHealth(health: ConsumerHealthMap): void;
}

/**
 * Socket.IO implementation. Global broadcasts feed the overview/health views;
 * per-order rooms (`order:<id>`) feed the order-detail timeline. Clients join
 * a room by emitting `subscribe` with an order id and leave via `unsubscribe`.
 */
export class SocketRealtimeGateway implements RealtimeGateway {
  constructor(private io: Server) {
    this.io.on('connection', (socket: Socket) => {
      socket.on('subscribe', (orderId: unknown) => {
        if (typeof orderId === 'string' && orderId) {
          socket.join(`order:${orderId}`);
        }
      });
      socket.on('unsubscribe', (orderId: unknown) => {
        if (typeof orderId === 'string' && orderId) {
          socket.leave(`order:${orderId}`);
        }
      });
    });
    logger.info({ event: 'realtime_gateway_ready' });
  }

  orderCreated(order: OrderSummary): void {
    this.io.emit(SOCKET_EVENTS.orderCreated, order);
  }

  orderUpdated(update: OrderUpdate): void {
    this.io.emit(SOCKET_EVENTS.orderUpdated, update);
  }

  orderEvent(orderId: string, event: OrderEventPayload): void {
    this.io.to(`order:${orderId}`).emit(SOCKET_EVENTS.orderEvent, { order_id: orderId, ...event });
  }

  consumerHealth(health: ConsumerHealthMap): void {
    this.io.emit(SOCKET_EVENTS.consumerHealth, health);
  }
}
