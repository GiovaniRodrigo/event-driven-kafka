# ARQUIVOS SRC - TYPESCRIPT (Copie e Cole)

> ⚠️ **Documento histórico — superado por `src/`.** O código deste guia foi **materializado e corrigido** em `src/` (rodável via `docker compose up --build`; ver [`docs/SETUP.md`](SETUP.md) e [`docs/ARCHITECTURE.md`](ARCHITECTURE.md)). Estes snippets copy-paste são mantidos como referência histórica do plano e **podem conter bugs já corrigidos** no código real — use `src/` como fonte da verdade.

## ARQUIVO 1: src/index.ts

```typescript
import express, { Express, Request, Response } from 'express';
import { OrderService } from './services/order-service';
import { OrderProducer } from './producers/order-producer';
import { PaymentConsumer } from './consumers/payment-consumer';
import { InventoryConsumer } from './consumers/inventory-consumer';
import { NotificationConsumer } from './consumers/notification-consumer';
import { DatabaseService } from './services/database';
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
app.get('/health', (req: Request, res: Response) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    version: '1.0.0',
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

    res.status(202).json({
      order_id: order.id,
      status: 'pending',
      message: 'Order accepted. Processing asynchronously.',
      created_at: order.created_at,
    });
  } catch (error) {
    logger.error({ event: 'post_order_error', error });
    res.status(500).json({ error: 'Failed to create order' });
  }
});

app.get('/orders/:order_id', async (req: Request, res: Response) => {
  try {
    const order = await db.getOrder(req.params.order_id);

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    res.json({
      order_id: order.id,
      user_id: order.user_id,
      status: order.status,
      created_at: order.created_at,
      updated_at: order.updated_at,
      total_amount: order.total_amount,
    });
  } catch (error) {
    logger.error({ event: 'get_order_error', error });
    res.status(500).json({ error: 'Failed to fetch order' });
  }
});

app.get('/metrics', async (req: Request, res: Response) => {
  try {
    const metrics = await db.getMetrics();
    res.json(metrics);
  } catch (error) {
    logger.error({ event: 'metrics_error', error });
    res.status(500).json({ error: 'Failed to fetch metrics' });
  }
});

async function start() {
  try {
    logger.info({ event: 'db_init_start' });
    await db.initialize();
    logger.info({ event: 'db_init_complete' });

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

process.on('SIGTERM', async () => {
  logger.info({ event: 'sigterm_received' });
  process.exit(0);
});

start();
```

---

## ARQUIVO 2: src/config.ts

```typescript
export const kafkaConfig = {
  brokers: (process.env.KAFKA_BROKER || 'localhost:9092').split(','),
  clientId: 'event-driven-api',
  connectionTimeout: 10000,
  requestTimeout: 30000,
};

export const producerConfig = {
  idempotent: true,
  maxInFlightRequests: 5,
  compression: 1,
  batchSize: 16384,
  lingerMs: 10,
  acks: -1,
  timeout: 30000,
};

export const consumerConfig = {
  sessionTimeout: 30000,
  heartbeatInterval: 10000,
  rebalanceTimeout: 60000,
  allowAutoTopicCreation: false,
};

export const topics = {
  orders: {
    name: 'orders',
    partitions: 3,
    replicationFactor: 1,
    config: ['compression.type=snappy', 'retention.ms=604800000'],
  },
  payments: {
    name: 'payments',
    partitions: 3,
    replicationFactor: 1,
  },
  inventory: {
    name: 'inventory',
    partitions: 3,
    replicationFactor: 1,
  },
  notifications: {
    name: 'notifications',
    partitions: 1,
    replicationFactor: 1,
  },
  dlq: {
    name: 'dlq',
    partitions: 1,
    replicationFactor: 1,
    config: ['retention.ms=2592000000'],
  },
};

export const databaseConfig = {
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME || 'event_driven',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
};
```

---

## ARQUIVO 3: src/types.ts

```typescript
export interface Order {
  id: string;
  user_id: string;
  items: OrderItem[];
  status: 'pending' | 'payment_processing' | 'payment_approved' | 'inventory_reserved' | 'completed' | 'failed';
  total_amount: number;
  created_at: Date;
  updated_at: Date;
}

export interface OrderItem {
  sku: string;
  name: string;
  price: number;
  quantity: number;
}

export interface KafkaEvent {
  event_id: string;
  event_type: string;
  correlation_id: string;
  timestamp: string;
  source_service: string;
  payload: any;
}

export interface PaymentEvent {
  event_id: string;
  order_id: string;
  user_id: string;
  amount: number;
  timestamp: string;
  correlation_id: string;
}

export interface InventoryEvent {
  event_id: string;
  order_id: string;
  items: OrderItem[];
  timestamp: string;
  correlation_id: string;
}
```

---

## ARQUIVO 4: src/utils/logger.ts

```typescript
import * as fs from 'fs';
import * as path from 'path';

const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

const levels: { [key: string]: number } = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

function shouldLog(level: string): boolean {
  return levels[level] >= levels[LOG_LEVEL];
}

function formatLog(level: string, data: any): string {
  return JSON.stringify({
    timestamp: new Date().toISOString(),
    level: level.toUpperCase(),
    ...data,
  });
}

export const logger = {
  debug: (data: any) => {
    if (shouldLog('debug')) {
      console.log(formatLog('debug', data));
    }
  },

  info: (data: any) => {
    if (shouldLog('info')) {
      console.log(formatLog('info', data));
    }
  },

  warn: (data: any) => {
    if (shouldLog('warn')) {
      console.warn(formatLog('warn', data));
    }
  },

  error: (data: any) => {
    if (shouldLog('error')) {
      console.error(formatLog('error', data));
    }
  },
};
```

---

## ARQUIVO 5: src/producers/base-producer.ts

```typescript
import { Kafka, Producer } from 'kafkajs';
import { kafkaConfig, producerConfig } from '../config';
import { logger } from '../utils/logger';

export abstract class BaseProducer {
  protected producer: Producer;
  protected kafka: Kafka;

  constructor(protected topicName: string) {
    this.kafka = new Kafka(kafkaConfig);
    this.producer = this.kafka.producer(producerConfig);
  }

  async connect() {
    await this.producer.connect();
    logger.info({ event: 'producer_connected', topic: this.topicName });
  }

  async disconnect() {
    await this.producer.disconnect();
    logger.info({ event: 'producer_disconnected', topic: this.topicName });
  }

  async emit(eventType: string, payload: any, key?: string) {
    const startTime = Date.now();

    try {
      const result = await this.producer.send({
        topic: this.topicName,
        messages: [
          {
            key: key || payload.event_id,
            value: JSON.stringify({
              type: eventType,
              ...payload,
            }),
            headers: {
              'correlation-id': payload.correlation_id || '',
              'event-id': payload.event_id || '',
              'source-service': 'order-service',
              'timestamp': new Date().toISOString(),
            },
          },
        ],
      });

      const duration = Date.now() - startTime;

      logger.info({
        event: 'message_produced',
        topic: this.topicName,
        type: eventType,
        event_id: payload.event_id,
        partition: result[0].partition,
        offset: result[0].offset,
        duration_ms: duration,
      });

      return result;
    } catch (error) {
      logger.error({
        event: 'produce_error',
        topic: this.topicName,
        type: eventType,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }
}
```

---

## ARQUIVO 6: src/producers/order-producer.ts

```typescript
import { BaseProducer } from './base-producer';
import { v4 as uuidv4 } from 'uuid';

export class OrderProducer extends BaseProducer {
  constructor() {
    super('orders');
  }

  async produceOrderCreatedEvent(order: any) {
    const eventId = `evt_${uuidv4()}`;
    const correlationId = order.id;

    await this.emit('order.created', {
      event_id: eventId,
      order_id: order.id,
      user_id: order.user_id,
      items: order.items,
      total_amount: order.total_amount,
      timestamp: new Date().toISOString(),
      correlation_id: correlationId,
    }, correlationId);
  }
}
```

---

## ARQUIVO 7: src/consumers/base-consumer.ts

```typescript
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { kafkaConfig, consumerConfig } from '../config';
import { logger } from '../utils/logger';
import { DatabaseService } from '../services/database';

export abstract class BaseConsumer {
  protected consumer: Consumer;
  protected kafka: Kafka;
  protected db: DatabaseService;
  protected retryCount: Map<string, number> = new Map();
  protected maxRetries = 3;

  constructor(
    protected topicName: string,
    protected groupId: string
  ) {
    this.kafka = new Kafka(kafkaConfig);
    this.consumer = this.kafka.consumer({
      ...consumerConfig,
      groupId: this.groupId,
    });
    this.db = new DatabaseService();
  }

  async start() {
    try {
      await this.consumer.connect();
      logger.info({
        event: 'consumer_connected',
        topic: this.topicName,
        group: this.groupId,
      });

      await this.consumer.subscribe({
        topic: this.topicName,
        fromBeginning: false,
      });

      await this.consumer.run({
        eachMessage: this.handleMessage.bind(this),
      });

      logger.info({
        event: 'consumer_started',
        topic: this.topicName,
        group: this.groupId,
      });
    } catch (error) {
      logger.error({
        event: 'consumer_start_error',
        topic: this.topicName,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      process.exit(1);
    }
  }

  protected async handleMessage(payload: EachMessagePayload) {
    const startTime = Date.now();
    const { message, partition, offset } = payload;

    let event: any;

    try {
      event = JSON.parse(message.value?.toString() || '{}');

      const eventId = event.event_id;
      const correlationId = event.correlation_id;

      const processed = await this.db.getProcessedEvent(eventId);
      if (processed) {
        logger.info({
          event: 'duplicate_event',
          event_id: eventId,
          topic: this.topicName,
        });
        return;
      }

      await this.processEvent(event);

      await this.db.markProcessed(eventId);

      this.retryCount.delete(eventId);

      const duration = Date.now() - startTime;
      logger.info({
        event: 'message_processed',
        topic: this.topicName,
        group: this.groupId,
        event_id: eventId,
        correlation_id: correlationId,
        partition,
        offset,
        duration_ms: duration,
      });
    } catch (error) {
      const eventId = event?.event_id || 'unknown';
      const currentRetry = (this.retryCount.get(eventId) || 0) + 1;
      this.retryCount.set(eventId, currentRetry);

      logger.error({
        event: 'process_error',
        topic: this.topicName,
        group: this.groupId,
        event_id: eventId,
        retry_count: currentRetry,
        max_retries: this.maxRetries,
        error: error instanceof Error ? error.message : 'Unknown error',
      });

      if (currentRetry >= this.maxRetries) {
        await this.handleError(event, error);
      } else {
        throw error;
      }
    }
  }

  protected abstract processEvent(event: any): Promise<void>;

  protected async handleError(event: any, error: Error) {
    logger.error({
      event: 'dlq_send',
      event_id: event?.event_id,
      correlation_id: event?.correlation_id,
      topic: this.topicName,
      reason: error.message,
      payload: JSON.stringify(event),
    });

    try {
      const kafka = new Kafka(kafkaConfig);
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'dlq',
        messages: [
          {
            key: event?.event_id,
            value: JSON.stringify({
              original_topic: this.topicName,
              original_event: event,
              error_message: error.message,
              timestamp: new Date().toISOString(),
            }),
          },
        ],
      });

      await producer.disconnect();

      logger.info({
        event: 'dlq_message_sent',
        event_id: event?.event_id,
      });
    } catch (dlqError) {
      logger.error({
        event: 'dlq_send_error',
        event_id: event?.event_id,
        error: dlqError instanceof Error ? dlqError.message : 'Unknown error',
      });
    }
  }

  async stop() {
    await this.consumer.disconnect();
    logger.info({
      event: 'consumer_stopped',
      topic: this.topicName,
      group: this.groupId,
    });
  }
}
```

---

## ARQUIVO 8: src/consumers/payment-consumer.ts

```typescript
import { BaseConsumer } from './base-consumer';
import { PaymentService } from '../services/payment-service';
import { logger } from '../utils/logger';

export class PaymentConsumer extends BaseConsumer {
  private paymentService: PaymentService;

  constructor() {
    super('payments', 'payment-processor-group');
    this.paymentService = new PaymentService(this.db);
  }

  protected async processEvent(event: any): Promise<void> {
    const startTime = Date.now();

    if (event.type === 'order.created') {
      logger.info({
        event: 'payment_processing_start',
        order_id: event.order_id,
        event_id: event.event_id,
      });

      const result = await this.paymentService.processPayment({
        order_id: event.order_id,
        user_id: event.user_id,
        amount: event.total_amount,
        items: event.items,
      });

      await this.db.updateOrderStatus(
        event.order_id,
        'payment_approved',
        { payment_id: result.payment_id }
      );

      const duration = Date.now() - startTime;
      logger.info({
        event: 'payment_processed',
        order_id: event.order_id,
        payment_id: result.payment_id,
        duration_ms: duration,
      });
    }
  }
}
```

---

## ARQUIVO 9: src/consumers/inventory-consumer.ts

```typescript
import { BaseConsumer } from './base-consumer';
import { InventoryService } from '../services/inventory-service';
import { logger } from '../utils/logger';

export class InventoryConsumer extends BaseConsumer {
  private inventoryService: InventoryService;

  constructor() {
    super('inventory', 'inventory-processor-group');
    this.inventoryService = new InventoryService(this.db);
  }

  protected async processEvent(event: any): Promise<void> {
    const startTime = Date.now();

    if (event.type === 'payment.approved') {
      logger.info({
        event: 'inventory_processing_start',
        order_id: event.order_id,
        event_id: event.event_id,
      });

      const result = await this.inventoryService.reserveInventory({
        order_id: event.order_id,
        items: event.items,
      });

      await this.db.updateOrderStatus(
        event.order_id,
        'inventory_reserved',
        { reservation_id: result.reservation_id }
      );

      const duration = Date.now() - startTime;
      logger.info({
        event: 'inventory_reserved',
        order_id: event.order_id,
        reservation_id: result.reservation_id,
        duration_ms: duration,
      });
    }
  }
}
```

---

## ARQUIVO 10: src/consumers/notification-consumer.ts

```typescript
import { BaseConsumer } from './base-consumer';
import { NotificationService } from '../services/notification-service';
import { logger } from '../utils/logger';

export class NotificationConsumer extends BaseConsumer {
  private notificationService: NotificationService;

  constructor() {
    super('notifications', 'notification-processor-group');
    this.notificationService = new NotificationService(this.db);
  }

  protected async processEvent(event: any): Promise<void> {
    const startTime = Date.now();

    if (event.type === 'order.created') {
      logger.info({
        event: 'notification_sending_start',
        order_id: event.order_id,
        user_id: event.user_id,
      });

      await this.notificationService.sendOrderConfirmation({
        order_id: event.order_id,
        user_id: event.user_id,
        items: event.items,
      });

      const duration = Date.now() - startTime;
      logger.info({
        event: 'notification_sent',
        order_id: event.order_id,
        duration_ms: duration,
      });
    } else if (event.type === 'payment.approved') {
      await this.notificationService.sendPaymentConfirmation({
        order_id: event.order_id,
        user_id: event.user_id,
        payment_id: event.payment_id,
      });
    }
  }
}
```

---

## ARQUIVO 11: src/services/order-service.ts

```typescript
import { DatabaseService } from './database';
import { OrderProducer } from '../producers/order-producer';
import { Order, OrderItem } from '../types';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger';

export class OrderService {
  private producer: OrderProducer;

  constructor(producer: OrderProducer, private db: DatabaseService) {
    this.producer = producer;
  }

  async createOrder(userId: string, items: OrderItem[]): Promise<Order> {
    const orderId = `ord_${uuidv4().slice(0, 8)}`;
    const totalAmount = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

    const now = new Date();

    const order: Order = {
      id: orderId,
      user_id: userId,
      items,
      status: 'pending',
      total_amount: totalAmount,
      created_at: now,
      updated_at: now,
    };

    await this.db.insertOrder(order);

    logger.info({
      event: 'order_created',
      order_id: orderId,
      user_id: userId,
      total_amount: totalAmount,
      items_count: items.length,
    });

    try {
      const eventId = `evt_${uuidv4().slice(0, 8)}`;
      await this.producer.emit('order.created', {
        event_id: eventId,
        order_id: orderId,
        user_id: userId,
        items,
        total_amount: totalAmount,
        timestamp: now.toISOString(),
        correlation_id: orderId,
      }, orderId);

      logger.info({
        event: 'order_event_emitted',
        order_id: orderId,
        event_id: eventId,
      });
    } catch (error) {
      logger.error({
        event: 'order_event_emit_error',
        order_id: orderId,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }

    return order;
  }
}
```

---

## ARQUIVO 12: src/services/database.ts

```typescript
import { Pool } from 'pg';
import { databaseConfig } from '../config';
import { Order } from '../types';
import { logger } from '../utils/logger';

export class DatabaseService {
  private pool: Pool;

  constructor() {
    this.pool = new Pool(databaseConfig);

    this.pool.on('error', (error) => {
      logger.error({ event: 'pool_error', error });
    });
  }

  async initialize() {
    try {
      const client = await this.pool.connect();
      await client.query('SELECT NOW()');
      client.release();

      logger.info({ event: 'database_connected' });

      await this.createTables();
    } catch (error) {
      logger.error({
        event: 'database_init_error',
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  private async createTables() {
    const client = await this.pool.connect();

    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS orders (
          id VARCHAR(50) PRIMARY KEY,
          user_id VARCHAR(50) NOT NULL,
          status VARCHAR(50) NOT NULL DEFAULT 'pending',
          total_amount DECIMAL(10, 2) NOT NULL,
          metadata JSONB,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS processed_events (
          event_id VARCHAR(100) PRIMARY KEY,
          processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS metrics (
          id SERIAL PRIMARY KEY,
          metric_name VARCHAR(100) NOT NULL,
          value DECIMAL(15, 2) NOT NULL,
          timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
      `);

      logger.info({ event: 'tables_created' });
    } finally {
      client.release();
    }
  }

  async insertOrder(order: Order): Promise<void> {
    const query = `
      INSERT INTO orders (id, user_id, status, total_amount, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6)
    `;

    await this.pool.query(query, [
      order.id,
      order.user_id,
      order.status,
      order.total_amount,
      order.created_at,
      order.updated_at,
    ]);
  }

  async getOrder(orderId: string): Promise<Order | null> {
    const query = 'SELECT * FROM orders WHERE id = $1';
    const result = await this.pool.query(query, [orderId]);

    if (result.rows.length === 0) {
      return null;
    }

    const row = result.rows[0];
    return {
      id: row.id,
      user_id: row.user_id,
      items: row.metadata?.items || [],
      status: row.status,
      total_amount: parseFloat(row.total_amount),
      created_at: new Date(row.created_at),
      updated_at: new Date(row.updated_at),
    };
  }

  async updateOrderStatus(orderId: string, status: string, metadata?: any): Promise<void> {
    const query = `
      UPDATE orders 
      SET status = $1, metadata = COALESCE(metadata, '{}'::jsonb) || $2, updated_at = CURRENT_TIMESTAMP
      WHERE id = $3
    `;

    await this.pool.query(query, [status, JSON.stringify(metadata || {}), orderId]);
  }

  async getProcessedEvent(eventId: string): Promise<boolean> {
    const query = 'SELECT event_id FROM processed_events WHERE event_id = $1';
    const result = await this.pool.query(query, [eventId]);
    return result.rows.length > 0;
  }

  async markProcessed(eventId: string): Promise<void> {
    const query = `
      INSERT INTO processed_events (event_id) VALUES ($1)
      ON CONFLICT (event_id) DO NOTHING
    `;

    await this.pool.query(query, [eventId]);
  }

  async getMetrics() {
    const query = `
      SELECT 
        COUNT(*) as total_orders,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_orders,
        AVG(total_amount) as avg_order_value
      FROM orders
    `;

    const result = await this.pool.query(query);
    return result.rows[0];
  }

  async disconnect(): Promise<void> {
    await this.pool.end();
    logger.info({ event: 'database_disconnected' });
  }
}
```

---

## ARQUIVO 13: src/services/payment-service.ts

```typescript
import { DatabaseService } from './database';
import { logger } from '../utils/logger';

export class PaymentService {
  constructor(private db: DatabaseService) {}

  async processPayment(paymentData: any): Promise<any> {
    try {
      const processingTime = Math.random() * 2000 + 1000;
      await new Promise(resolve => setTimeout(resolve, processingTime));

      const paymentId = `pay_${Date.now()}`;

      logger.info({
        event: 'payment_processed',
        order_id: paymentData.order_id,
        payment_id: paymentId,
        amount: paymentData.amount,
      });

      return { payment_id: paymentId, status: 'approved' };
    } catch (error) {
      logger.error({
        event: 'payment_error',
        order_id: paymentData.order_id,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }
}
```

---

## ARQUIVO 14: src/services/inventory-service.ts

```typescript
import { DatabaseService } from './database';
import { logger } from '../utils/logger';

export class InventoryService {
  constructor(private db: DatabaseService) {}

  async reserveInventory(reservationData: any): Promise<any> {
    try {
      const processingTime = Math.random() * 1000 + 500;
      await new Promise(resolve => setTimeout(resolve, processingTime));

      const reservationId = `res_${Date.now()}`;

      logger.info({
        event: 'inventory_reserved',
        order_id: reservationData.order_id,
        reservation_id: reservationId,
        items_count: reservationData.items.length,
      });

      return { reservation_id: reservationId, status: 'reserved' };
    } catch (error) {
      logger.error({
        event: 'inventory_error',
        order_id: reservationData.order_id,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }
}
```

---

## ARQUIVO 15: src/services/notification-service.ts

```typescript
import { DatabaseService } from './database';
import { logger } from '../utils/logger';

export class NotificationService {
  constructor(private db: DatabaseService) {}

  async sendOrderConfirmation(data: any): Promise<void> {
    try {
      await new Promise(resolve => setTimeout(resolve, 100));

      logger.info({
        event: 'order_confirmation_sent',
        order_id: data.order_id,
        user_id: data.user_id,
      });
    } catch (error) {
      logger.error({
        event: 'notification_error',
        order_id: data.order_id,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  async sendPaymentConfirmation(data: any): Promise<void> {
    try {
      await new Promise(resolve => setTimeout(resolve, 100));

      logger.info({
        event: 'payment_confirmation_sent',
        order_id: data.order_id,
        payment_id: data.payment_id,
      });
    } catch (error) {
      logger.error({
        event: 'notification_error',
        order_id: data.order_id,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }
}
```

---

## ✅ Checklist

- [ ] src/index.ts criado
- [ ] src/config.ts criado
- [ ] src/types.ts criado
- [ ] src/utils/logger.ts criado
- [ ] src/producers/base-producer.ts criado
- [ ] src/producers/order-producer.ts criado
- [ ] src/consumers/base-consumer.ts criado
- [ ] src/consumers/payment-consumer.ts criado
- [ ] src/consumers/inventory-consumer.ts criado
- [ ] src/consumers/notification-consumer.ts criado
- [ ] src/services/order-service.ts criado
- [ ] src/services/database.ts criado
- [ ] src/services/payment-service.ts criado
- [ ] src/services/inventory-service.ts criado
- [ ] src/services/notification-service.ts criado

**15 arquivos .ts = PRONTO!**

Próximo: **05_DOCKER_E_DOCS.md**
