import express, { Express, Request, Response } from 'express';
import { Kafka } from 'kafkajs';
import { OrderService } from './services/order-service';
import { OrderProducer } from './producers/order-producer';
import { PaymentConsumer } from './consumers/payment-consumer';
import { InventoryConsumer } from './consumers/inventory-consumer';
import { NotificationConsumer } from './consumers/notification-consumer';
import { DatabaseService } from './services/database';
import { kafkaConfig, topics } from './config';
import { logger } from './utils/logger';

const app: Express = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const db = new DatabaseService();
const orderProducer = new OrderProducer();
const orderService = new OrderService(orderProducer, db);

const paymentConsumer = new PaymentConsumer();
const inventoryConsumer = new InventoryConsumer();
const notificationConsumer = new NotificationConsumer();

// ROUTES

app.get('/health', async (_req: Request, res: Response) => {
  const dbHealthy = await db.ping();
  res.json({
    status: dbHealthy ? 'ok' : 'degraded',
    timestamp: new Date().toISOString(),
    version: '1.0.0',
    database: dbHealthy ? 'healthy' : 'down',
    consumers: {
      payment: paymentConsumer.health,
      inventory: inventoryConsumer.health,
      notification: notificationConsumer.health,
    },
  });
});

app.post('/orders', async (req: Request, res: Response) => {
  try {
    const { user_id, items } = req.body;

    if (!user_id || !items || items.length === 0) {
      return res.status(400).json({
        error: 'Missing required fields: user_id, items',
      });
    }

    const order = await orderService.createOrder(user_id, items);

    return res.status(202).json({
      order_id: order.id,
      status: 'pending',
      message: 'Order accepted. Processing asynchronously.',
      created_at: order.created_at,
    });
  } catch (error) {
    logger.error({ event: 'post_order_error', error });
    return res.status(500).json({ error: 'Failed to create order' });
  }
});

app.get('/orders', async (_req: Request, res: Response) => {
  try {
    const orders = await db.listOrders();
    return res.json({
      orders: orders.map((o) => ({
        order_id: o.id,
        user_id: o.user_id,
        status: o.status,
        total_amount: o.total_amount,
        created_at: o.created_at,
      })),
    });
  } catch (error) {
    logger.error({ event: 'list_orders_error', error });
    return res.status(500).json({ error: 'Failed to list orders' });
  }
});

app.get('/orders/:order_id', async (req: Request, res: Response) => {
  try {
    const order = await db.getOrder(req.params.order_id);

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    const events = await db.getOrderEvents(order.id);

    return res.json({
      order_id: order.id,
      user_id: order.user_id,
      status: order.status,
      items: order.items,
      total_amount: order.total_amount,
      created_at: order.created_at,
      updated_at: order.updated_at,
      events,
    });
  } catch (error) {
    logger.error({ event: 'get_order_error', error });
    return res.status(500).json({ error: 'Failed to fetch order' });
  }
});

app.get('/metrics', async (_req: Request, res: Response) => {
  try {
    const metrics = await db.getMetrics();
    return res.json(metrics);
  } catch (error) {
    logger.error({ event: 'metrics_error', error });
    return res.status(500).json({ error: 'Failed to fetch metrics' });
  }
});

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
  try {
    await Promise.allSettled([
      paymentConsumer.stop(),
      inventoryConsumer.stop(),
      notificationConsumer.stop(),
      orderProducer.disconnect(),
    ]);
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
    logger.info({ event: 'consumers_started' });

    app.listen(PORT, () => {
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
