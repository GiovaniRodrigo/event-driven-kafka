import express, { Express, Request, Response } from 'express';
import { Order, OrderEvent, OrderItem } from './types';
import { ConsumerHealthMap } from '@kafka-demo/contracts';
import { logger } from './utils/logger';

/** Read model the HTTP routes need — a narrow view of DatabaseService. */
export interface OrdersReadModel {
  ping(): Promise<boolean>;
  listOrders(limit?: number): Promise<Order[]>;
  getOrder(orderId: string): Promise<Order | null>;
  getOrderEvents(orderId: string): Promise<OrderEvent[]>;
  getMetrics(): Promise<unknown>;
}

/** The only thing the routes need to create an order. */
export interface OrderCreator {
  createOrder(userId: string, items: OrderItem[]): Promise<{ id: string; created_at: Date }>;
}

export interface AppDependencies {
  orderService: OrderCreator;
  db: OrdersReadModel;
  getConsumerHealth: () => ConsumerHealthMap;
}

/**
 * Builds the Express app from injected dependencies so routes can be tested
 * in isolation (supertest + fakes) without Kafka, Postgres, or Socket.IO.
 */
export function createApp(deps: AppDependencies): Express {
  const { orderService, db, getConsumerHealth } = deps;
  const app = express();
  app.use(express.json());

  app.get('/health', async (_req: Request, res: Response) => {
    const dbHealthy = await db.ping();
    res.json({
      status: dbHealthy ? 'ok' : 'degraded',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
      database: dbHealthy ? 'healthy' : 'down',
      consumers: getConsumerHealth(),
    });
  });

  app.post('/orders', async (req: Request, res: Response) => {
    try {
      const { user_id, items } = req.body;

      if (!user_id || !items || items.length === 0) {
        return res.status(400).json({ error: 'Missing required fields: user_id, items' });
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

  return app;
}
