import http from 'http';
import { Server } from 'socket.io';
import { Kafka } from 'kafkajs';
import { createApp } from './app';
import { OrderService } from './services/order-service';
import { DatabaseService } from './services/database';
import { OutboxRelay } from './infrastructure/outbox/outbox-relay';
import { SagaOrchestrator } from './saga/saga-orchestrator';
import { PaymentConsumer } from './consumers/payment-consumer';
import { InventoryConsumer } from './consumers/inventory-consumer';
import { FraudConsumer } from './consumers/fraud-consumer';
import { ShippingConsumer } from './consumers/shipping-consumer';
import { NotificationConsumer } from './consumers/notification-consumer';
import { ProjectionConsumer } from './application/projections/projection-consumer';
import { SocketRealtimeGateway } from './realtime/realtime-gateway';
import { EventReplayService } from './replay/event-replay-service';
import { ChaosEngine } from './chaos/chaos-engine';
import { kafkaConfig, topics, env } from './config';
import { logger } from './utils/logger';

const PORT = env.PORT;
const METRICS_BROADCAST_MS = 5000;

const db = new DatabaseService();
const outboxRelay = new OutboxRelay(db);
const orderService = new OrderService(db, outboxRelay);

const paymentConsumer = new PaymentConsumer(db);
const inventoryConsumer = new InventoryConsumer(db);
const fraudConsumer = new FraudConsumer(db);
const shippingConsumer = new ShippingConsumer(db);
const notificationConsumer = new NotificationConsumer(db);
const sagaOrchestrator = new SagaOrchestrator(db);

const server = http.createServer();
const io = new Server(server, { cors: { origin: '*' } });
const realtimeGateway = new SocketRealtimeGateway(io);
const projectionConsumer = new ProjectionConsumer(realtimeGateway, db);
const replayService = new EventReplayService(db, projectionConsumer);

function getConsumersStatus(): Record<string, any> {
  return {
    payment: paymentConsumer.metrics,
    inventory: inventoryConsumer.metrics,
    fraud: fraudConsumer.metrics,
    shipping: shippingConsumer.metrics,
    notification: notificationConsumer.metrics,
    saga: sagaOrchestrator.metrics,
    projection: projectionConsumer.metrics,
  };
}

async function isKafkaReady(): Promise<boolean> {
  const admin = new Kafka(kafkaConfig).admin();
  try {
    await admin.connect();
    await admin.listTopics();
    return true;
  } catch {
    return false;
  } finally {
    await admin.disconnect();
  }
}

const app = createApp({
  orderService,
  db,
  replayService,
  chaosEngine: ChaosEngine.getInstance(),
  getConsumersStatus,
  isKafkaReady,
});

server.on('request', app);

let metricsTimer: NodeJS.Timeout | undefined;

async function createTopics() {
  const admin = new Kafka(kafkaConfig).admin();
  await admin.connect();
  try {
    const existingTopics = await admin.listTopics();
    const topicsToCreate = Object.values(topics)
      .filter((t) => !existingTopics.includes(t.name))
      .map((t: any) => ({
        topic: t.name,
        numPartitions: t.partitions,
        replicationFactor: t.replicationFactor,
        configEntries: t.config
          ? t.config.map((c: string) => {
              const [name, value] = c.split('=');
              return { name, value };
            })
          : [],
      }));

    if (topicsToCreate.length > 0) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: topicsToCreate,
      });
      logger.info({ event: 'kafka_topics_created', topics: topicsToCreate.map((t) => t.topic) });
    } else {
      logger.info({ event: 'kafka_topics_verified', count: existingTopics.length });
    }
  } finally {
    await admin.disconnect();
  }
}

let isShuttingDown = false;

async function shutdown(signal: string) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  logger.info({ event: 'graceful_shutdown_start', signal });

  if (metricsTimer) {
    clearInterval(metricsTimer);
  }

  // 1. Stop receiving new HTTP requests
  server.close(() => {
    logger.info({ event: 'http_server_closed' });
  });

  // 2. Stop Outbox Relay and Consumers
  try {
    await Promise.allSettled([
      outboxRelay.stop(),
      paymentConsumer.stop(),
      inventoryConsumer.stop(),
      fraudConsumer.stop(),
      shippingConsumer.stop(),
      notificationConsumer.stop(),
      sagaOrchestrator.stop(),
      projectionConsumer.stop(),
      replayService.disconnect(),
    ]);
    logger.info({ event: 'all_consumers_stopped' });

    // 3. Close Socket.IO
    io.close();

    // 4. Disconnect Database Pool
    await db.disconnect();
    logger.info({ event: 'graceful_shutdown_complete' });
  } catch (error) {
    logger.error({ event: 'shutdown_error', error: (error as Error).message });
  } finally {
    process.exit(0);
  }
}

async function start() {
  try {
    logger.info({ event: 'startup_init_database' });
    await db.initialize();

    logger.info({ event: 'startup_ensure_kafka_topics' });
    await createTopics();

    logger.info({ event: 'startup_connecting_services' });
    await replayService.connect();
    await outboxRelay.start();

    logger.info({ event: 'startup_starting_consumers' });
    await Promise.all([
      paymentConsumer.start(),
      inventoryConsumer.start(),
      fraudConsumer.start(),
      shippingConsumer.start(),
      notificationConsumer.start(),
      sagaOrchestrator.start(),
      projectionConsumer.start(),
    ]);

    metricsTimer = setInterval(async () => {
      try {
        const metrics = await db.getMetrics();
        const sagas = await db.listSagas(50);
        const activeSagas = sagas.filter((s) => !['COMPLETED', 'CANCELLED', 'FAILED'].includes(s.state)).length;
        const dlqCount = (await db.listDLQMessages(50)).length;

        io.emit('metrics:update', {
          ...metrics,
          active_sagas: activeSagas,
          dlq_count: dlqCount,
          timestamp: new Date().toISOString(),
        });
      } catch {}
    }, METRICS_BROADCAST_MS);

    server.listen(PORT, () => {
      logger.info({
        event: 'server_started',
        port: PORT,
        environment: env.NODE_ENV,
        timestamp: new Date().toISOString(),
      });
    });
  } catch (error) {
    logger.error({ event: 'startup_fatal_error', error: (error as Error).message });
    process.exit(1);
  }
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

if (process.env.NODE_ENV !== 'test') {
  start();
}
