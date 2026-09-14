import { Kafka, Consumer } from 'kafkajs';
import { kafkaConfig, consumerConfig } from '../config';
import { logger } from '../utils/logger';
import { RealtimeGateway } from './realtime-gateway';
import { OrderStatus } from '../types';

/** Result topics the read-side consumer watches to mirror pipeline progress. */
const SUBSCRIBE_TOPICS = ['orders', 'payments', 'inventory', 'notifications'];

/** The status an order reaches once a given event type has been produced. */
const STATUS_BY_TYPE: Record<string, OrderStatus> = {
  'payment.approved': 'payment_approved',
  'inventory.reserved': 'inventory_reserved',
  'notification.sent': 'completed',
};

/** The topic each event type is produced to (for the timeline). */
const TOPIC_BY_TYPE: Record<string, string> = {
  'order.created': 'orders',
  'payment.approved': 'payments',
  'inventory.reserved': 'inventory',
  'notification.sent': 'notifications',
};

/**
 * Dedicated read-side consumer that bridges Kafka pipeline events to the
 * real-time gateway. It is decoupled from the business consumers: it only
 * observes their result topics and re-broadcasts progress to clients.
 */
export class RealtimeConsumer {
  private kafka: Kafka;
  private consumer?: Consumer;

  constructor(private gateway: RealtimeGateway) {
    this.kafka = new Kafka(kafkaConfig);
  }

  /** Translate one raw Kafka message value into gateway broadcasts. */
  handleEvent(raw: string): void {
    let event: any;
    try {
      event = JSON.parse(raw);
    } catch {
      return;
    }

    if (!event || typeof event.type !== 'string' || typeof event.order_id !== 'string') {
      return;
    }

    const topic = TOPIC_BY_TYPE[event.type];
    if (!topic) {
      return;
    }

    const orderId = event.order_id as string;
    const timestamp = typeof event.timestamp === 'string' ? event.timestamp : new Date().toISOString();

    if (event.type === 'order.created') {
      this.gateway.orderCreated({
        order_id: orderId,
        user_id: typeof event.user_id === 'string' ? event.user_id : 'unknown',
        status: 'pending',
        total_amount: Number(event.total_amount) || 0,
      });
    } else {
      this.gateway.orderUpdated({ order_id: orderId, status: STATUS_BY_TYPE[event.type] });
    }

    this.gateway.orderEvent(orderId, { event_type: event.type, topic, timestamp });
  }

  async start(): Promise<void> {
    const consumer = this.kafka.consumer({ ...consumerConfig, groupId: 'realtime-broadcaster-group' });
    this.consumer = consumer;
    await consumer.connect();
    await consumer.subscribe({ topics: SUBSCRIBE_TOPICS, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }) => {
        this.handleEvent(message.value?.toString() || '');
      },
    });
    logger.info({ event: 'realtime_consumer_started', topics: SUBSCRIBE_TOPICS });
  }

  async stop(): Promise<void> {
    if (this.consumer) {
      await this.consumer.disconnect();
    }
  }
}
