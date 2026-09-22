import express, { Express, Request, Response } from 'express';
import { DatabaseService } from './services/database';
import { OrderService } from './services/order-service';
import { ChaosEngine } from './chaos/chaos-engine';
import { EventReplayService } from './replay/event-replay-service';
import { logger } from './utils/logger';

export interface AppDependencies {
  orderService: OrderService;
  db: DatabaseService;
  replayService?: EventReplayService;
  chaosEngine?: ChaosEngine;
  getConsumersStatus: () => Record<string, any>;
  isKafkaReady?: () => Promise<boolean>;
}

export function createApp(deps: AppDependencies): Express {
  const { orderService, db, replayService, chaosEngine = ChaosEngine.getInstance(), getConsumersStatus, isKafkaReady } = deps;
  const app = express();
  app.use(express.json());

  let httpRequestsTotal = 0;
  const httpDurations: number[] = [];

  app.use((_req: Request, res: Response, next) => {
    const start = Date.now();
    res.on('finish', () => {
      httpRequestsTotal++;
      const duration = Date.now() - start;
      if (httpDurations.length < 10000) {
        httpDurations.push(duration);
      } else {
        httpDurations[Math.floor(Math.random() * httpDurations.length)] = duration;
      }
    });
    next();
  });

  // --- HEALTH & READINESS ---

  // Liveness probe (is Node.js process alive?)
  app.get('/health', async (_req: Request, res: Response) => {
    const dbHealthy = await db.ping();
    const consumers = getConsumersStatus();
    const hasDegraded = Object.values(consumers).some((c: any) => c.status === 'degraded');
    const hasDown = Object.values(consumers).some((c: any) => c.status === 'down');

    const overallStatus = !dbHealthy || hasDown ? 'down' : hasDegraded ? 'degraded' : 'ok';

    return res.status(overallStatus === 'down' ? 503 : 200).json({
      status: overallStatus,
      timestamp: new Date().toISOString(),
      version: '1.0.0',
      database: dbHealthy ? 'healthy' : 'down',
      consumers,
    });
  });

  // Readiness probe (are DB & Kafka ready to accept traffic?)
  app.get('/ready', async (_req: Request, res: Response) => {
    const dbReady = await db.ping();
    let kafkaReady = true;
    if (isKafkaReady) {
      try {
        kafkaReady = await isKafkaReady();
      } catch {
        kafkaReady = false;
      }
    }

    const ready = dbReady && kafkaReady;
    return res.status(ready ? 200 : 503).json({
      ready,
      database: dbReady ? 'ready' : 'not_ready',
      kafka: kafkaReady ? 'ready' : 'not_ready',
      timestamp: new Date().toISOString(),
    });
  });

  // --- ORDER COMMAND & QUERY API ---

  app.post('/orders', async (req: Request, res: Response) => {
    try {
      const { user_id, items, currency } = req.body;

      if (!user_id || !items || !Array.isArray(items) || items.length === 0) {
        return res.status(400).json({ error: 'Missing required fields: user_id, items (non-empty array)' });
      }

      for (const item of items) {
        if (!item.sku || !item.name || typeof item.price !== 'number' || typeof item.quantity !== 'number') {
          return res.status(400).json({ error: 'Invalid item format. Required: sku, name, price, quantity' });
        }
      }

      const order = await orderService.createOrder(user_id, items, { currency });

      return res.status(202).json({
        order_id: order.id,
        status: 'pending',
        message: 'Order accepted. Processing asynchronously via Kafka EDA Pipeline.',
        total_amount: order.total_amount,
        created_at: order.created_at,
      });
    } catch (error) {
      logger.error({ event: 'post_order_error', error: (error as Error).message });
      return res.status(500).json({ error: 'Failed to create order' });
    }
  });

  app.get('/orders', async (req: Request, res: Response) => {
    try {
      const limit = Number(req.query.limit) || 50;
      const orders = await db.listOrders(limit);
      return res.json({
        total: orders.length,
        orders: orders.map((o) => ({
          order_id: o.id,
          user_id: o.user_id,
          status: o.status,
          total_amount: o.total_amount,
          items: o.items,
          created_at: o.created_at,
        })),
      });
    } catch (error) {
      logger.error({ event: 'list_orders_error', error: (error as Error).message });
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
      const saga = await db.getSagaByAggregateId(order.id);

      return res.json({
        order_id: order.id,
        user_id: order.user_id,
        status: order.status,
        items: order.items,
        total_amount: order.total_amount,
        created_at: order.created_at,
        updated_at: order.updated_at,
        saga_state: saga?.state,
        events,
      });
    } catch (error) {
      logger.error({ event: 'get_order_error', error: (error as Error).message });
      return res.status(500).json({ error: 'Failed to fetch order' });
    }
  });

  app.get('/orders/:order_id/events', async (req: Request, res: Response) => {
    try {
      const events = await db.getEventsByAggregateId(req.params.order_id);
      return res.json({
        order_id: req.params.order_id,
        count: events.length,
        events,
      });
    } catch (error) {
      logger.error({ event: 'get_order_events_error', error: (error as Error).message });
      return res.status(500).json({ error: 'Failed to fetch order events' });
    }
  });

  app.get('/orders/:order_id/timeline', async (req: Request, res: Response) => {
    try {
      const events = await db.getOrderEvents(req.params.order_id);
      return res.json({
        order_id: req.params.order_id,
        timeline: events,
      });
    } catch (error) {
      logger.error({ event: 'get_order_timeline_error', error: (error as Error).message });
      return res.status(500).json({ error: 'Failed to fetch timeline' });
    }
  });

  // --- SAGAS & METRICS ---

  app.get('/sagas', async (req: Request, res: Response) => {
    try {
      const limit = Number(req.query.limit) || 50;
      const sagas = await db.listSagas(limit);
      return res.json({ total: sagas.length, sagas });
    } catch (error) {
      return res.status(500).json({ error: 'Failed to list sagas' });
    }
  });

  app.get('/sagas/:id', async (req: Request, res: Response) => {
    try {
      const saga = await db.getSagaInstance(req.params.id);
      if (!saga) return res.status(404).json({ error: 'Saga not found' });
      return res.json(saga);
    } catch (error) {
      return res.status(500).json({ error: 'Failed to fetch saga' });
    }
  });

  app.get('/consumers', (_req: Request, res: Response) => {
    const consumers = getConsumersStatus();
    return res.json(consumers);
  });

  app.get('/metrics', async (_req: Request, res: Response) => {
    try {
      const metrics = await db.getMetrics();
      const sagas = await db.listSagas(100);
      const dlqCount = (await db.listDLQMessages(100)).length;

      const activeSagas = sagas.filter((s) => !['COMPLETED', 'CANCELLED', 'FAILED'].includes(s.state)).length;

      const sortedDurations = [...httpDurations].sort((a, b) => a - b);
      const httpP50 = sortedDurations[Math.floor(sortedDurations.length * 0.5)] || 0;
      const httpP95 = sortedDurations[Math.floor(sortedDurations.length * 0.95)] || 0;
      const httpP99 = sortedDurations[Math.floor(sortedDurations.length * 0.99)] || 0;

      return res.json({
        ...metrics,
        http_requests_total: httpRequestsTotal,
        http_request_duration: {
          p50_ms: httpP50,
          p95_ms: httpP95,
          p99_ms: httpP99,
          samples: sortedDurations.length,
        },
        active_sagas: activeSagas,
        unresolved_dlq_count: dlqCount,
        consumers: getConsumersStatus(),
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      return res.status(500).json({ error: 'Failed to fetch metrics' });
    }
  });

  // --- DEAD LETTER QUEUE (DLQ) API ---

  app.get('/dlq', async (req: Request, res: Response) => {
    try {
      const limit = Number(req.query.limit) || 50;
      const status = (req.query.status as string) || 'UNRESOLVED';
      const messages = await db.listDLQMessages(limit, status);
      return res.json({ total: messages.length, messages });
    } catch (error) {
      return res.status(500).json({ error: 'Failed to fetch DLQ messages' });
    }
  });

  app.get('/dlq/:id', async (req: Request, res: Response) => {
    try {
      const message = await db.getDLQMessage(req.params.id);
      if (!message) return res.status(404).json({ error: 'DLQ message not found' });
      return res.json(message);
    } catch (error) {
      return res.status(500).json({ error: 'Failed to fetch DLQ message' });
    }
  });

  app.post('/dlq/:id/replay', async (req: Request, res: Response) => {
    try {
      if (!replayService) {
        return res.status(503).json({ error: 'Replay service not available' });
      }
      const result = await replayService.replayDLQ(req.params.id);
      return res.json(result);
    } catch (error) {
      return res.status(500).json({ error: (error as Error).message });
    }
  });

  // --- REPLAY API ---

  app.post('/replay', async (req: Request, res: Response) => {
    try {
      if (!replayService) {
        return res.status(503).json({ error: 'Replay service not available' });
      }
      const { aggregate_id } = req.body;
      if (!aggregate_id) {
        return res.status(400).json({ error: 'aggregate_id is required for replay' });
      }

      const result = await replayService.replayAggregate(aggregate_id);
      return res.json(result);
    } catch (error) {
      return res.status(500).json({ error: (error as Error).message });
    }
  });

  // --- CHAOS CONTROLS API ---

  app.get('/chaos/status', (_req: Request, res: Response) => {
    return res.json(chaosEngine.getStatus());
  });

  app.post('/chaos/payment/failure', (req: Request, res: Response) => {
    const enabled = req.body.enabled !== false;
    chaosEngine.setFault({ paymentFailure: enabled });
    return res.json({ message: `Chaos Payment Failure set to ${enabled}`, status: chaosEngine.getStatus() });
  });

  app.post('/chaos/inventory/failure', (req: Request, res: Response) => {
    const enabled = req.body.enabled !== false;
    chaosEngine.setFault({ inventoryFailure: enabled });
    return res.json({ message: `Chaos Inventory Failure set to ${enabled}`, status: chaosEngine.getStatus() });
  });

  app.post('/chaos/fraud/rejection', (req: Request, res: Response) => {
    const enabled = req.body.enabled !== false;
    chaosEngine.setFault({ fraudRejection: enabled });
    return res.json({ message: `Chaos Fraud Rejection set to ${enabled}`, status: chaosEngine.getStatus() });
  });

  app.post('/chaos/fraud/latency', (req: Request, res: Response) => {
    const latencyMs = Number(req.body.latency_ms) || 2000;
    chaosEngine.setFault({ fraudLatencyMs: latencyMs });
    return res.json({ message: `Chaos Fraud Latency set to ${latencyMs}ms`, status: chaosEngine.getStatus() });
  });

  app.post('/chaos/shipping/failure', (req: Request, res: Response) => {
    const enabled = req.body.enabled !== false;
    chaosEngine.setFault({ shippingFailure: enabled });
    return res.json({ message: `Chaos Shipping Failure set to ${enabled}`, status: chaosEngine.getStatus() });
  });

  app.post('/chaos/notification/down', (req: Request, res: Response) => {
    const enabled = req.body.enabled !== false;
    chaosEngine.setFault({ notificationFailure: enabled });
    return res.json({ message: `Chaos Notification Down set to ${enabled}`, status: chaosEngine.getStatus() });
  });

  app.post('/chaos/reset', (_req: Request, res: Response) => {
    chaosEngine.reset();
    return res.json({ message: 'All chaos faults reset to default (disabled)', status: chaosEngine.getStatus() });
  });

  return app;
}
