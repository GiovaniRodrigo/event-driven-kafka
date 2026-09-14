import http from 'http';
import { Server } from 'socket.io';
import { Kafka } from 'kafkajs';
import { createApp } from './app';
import { OrderService } from './services/order-service';
import { OrderProducer } from './producers/order-producer';
import { PaymentConsumer } from './consumers/payment-consumer';
import { InventoryConsumer } from './consumers/inventory-consumer';
import { NotificationConsumer } from './consumers/notification-consumer';
import { DatabaseService } from './services/database';
import { SocketRealtimeGateway } from './realtime/realtime-gateway';
import { RealtimeConsumer } from './realtime/realtime-consumer';
import { ConsumerHealthMap } from '@kafka-demo/contracts';
import { kafkaConfig, topics } from './config';
import { logger } from './utils/logger';

const PORT = process.env.PORT || 3000;
const HEALTH_BROADCAST_MS = 10000;

const db = new DatabaseService();
const orderProducer = new OrderProducer();
const orderService = new OrderService(orderProducer, db);

const paymentConsumer = new PaymentConsumer();
const inventoryConsumer = new InventoryConsumer();
const notificationConsumer = new NotificationConsumer();

function getConsumerHealth(): ConsumerHealthMap {
  return {
    payment: paymentConsumer.health,
    inventory: inventoryConsumer.health,
    notification: notificationConsumer.health,
  };
}

const app = createApp({ orderService, db, getConsumerHealth });
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
const realtimeGateway = new SocketRealtimeGateway(io);
const realtimeConsumer = new RealtimeConsumer(realtimeGateway);

let healthTimer: NodeJS.Timeout | undefined;

async function createTopics() {
  const admin = new Kafka(kafkaConfig).admin();
  await admin.connect();
  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: Object.values(topics).map((t) => ({
        topic: t.name,
        numPartitions: t.partitions,
        replicationFactor: t.replicationFactor,
      })),
    });
    logger.info({ event: 'topics_ensured', topics: Object.values(topics).map((t) => t.name) });
  } finally {
    await admin.disconnect();
  }
}

async function shutdown(signal: string) {
  logger.info({ event: 'shutdown_start', signal });
  if (healthTimer) {
    clearInterval(healthTimer);
  }
  try {
    await Promise.allSettled([
      paymentConsumer.stop(),
      inventoryConsumer.stop(),
      notificationConsumer.stop(),
      realtimeConsumer.stop(),
      orderProducer.disconnect(),
    ]);
    io.close();
    await db.disconnect();
  } catch (error) {
    logger.error({ event: 'shutdown_error', error });
  } finally {
    process.exit(0);
  }
}

async function start() {
  try {
    logger.info({ event: 'db_init_start' });
    await db.initialize();
    logger.info({ event: 'db_init_complete' });

    logger.info({ event: 'topics_init_start' });
    await createTopics();

    logger.info({ event: 'producer_connect_start' });
    await orderProducer.connect();

    logger.info({ event: 'consumers_start' });
    await paymentConsumer.start();
    await inventoryConsumer.start();
    await notificationConsumer.start();
    await realtimeConsumer.start();
    logger.info({ event: 'consumers_started' });

    healthTimer = setInterval(() => realtimeGateway.consumerHealth(getConsumerHealth()), HEALTH_BROADCAST_MS);

    server.listen(PORT, () => {
      logger.info({
        event: 'server_started',
        port: PORT,
        environment: process.env.NODE_ENV || 'development',
        timestamp: new Date().toISOString(),
      });
    });
  } catch (error) {
    logger.error({ event: 'startup_error', error });
    process.exit(1);
  }
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

start();
