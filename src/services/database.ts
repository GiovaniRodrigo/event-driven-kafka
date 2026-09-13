import { Pool } from 'pg';
import { databaseConfig } from '../config';
import { Order, OrderEvent } from '../types';
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
    const maxAttempts = 10;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const client = await this.pool.connect();
        await client.query('SELECT NOW()');
        client.release();

        logger.info({ event: 'database_connected', attempt });

        await this.createTables();
        return;
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        if (attempt === maxAttempts) {
          logger.error({ event: 'database_init_error', error: message });
          throw error;
        }
        logger.warn({ event: 'database_connect_retry', attempt, error: message });
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }
  }

  async ping(): Promise<boolean> {
    try {
      await this.pool.query('SELECT 1');
      return true;
    } catch {
      return false;
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
        CREATE TABLE IF NOT EXISTS order_events (
          id SERIAL PRIMARY KEY,
          order_id VARCHAR(50) NOT NULL,
          event_type VARCHAR(100) NOT NULL,
          topic VARCHAR(100) NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_order_events_order_id
        ON order_events (order_id, created_at);
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
    // Persist items inside metadata so getOrder can reconstruct them.
    const query = `
      INSERT INTO orders (id, user_id, status, total_amount, metadata, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
    `;

    await this.pool.query(query, [
      order.id,
      order.user_id,
      order.status,
      order.total_amount,
      JSON.stringify({ items: order.items }),
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

    return this.mapOrderRow(result.rows[0]);
  }

  async listOrders(limit = 50): Promise<Order[]> {
    const query = 'SELECT * FROM orders ORDER BY created_at DESC LIMIT $1';
    const result = await this.pool.query(query, [limit]);
    return result.rows.map((row) => this.mapOrderRow(row));
  }

  private mapOrderRow(row: any): Order {
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

  async recordEvent(orderId: string, eventType: string, topic: string): Promise<void> {
    const query = `
      INSERT INTO order_events (order_id, event_type, topic)
      VALUES ($1, $2, $3)
    `;
    await this.pool.query(query, [orderId, eventType, topic]);
  }

  async getOrderEvents(orderId: string): Promise<OrderEvent[]> {
    const query = `
      SELECT event_type, topic, created_at
      FROM order_events
      WHERE order_id = $1
      ORDER BY created_at ASC
    `;
    const result = await this.pool.query(query, [orderId]);
    return result.rows.map((row) => ({
      event_type: row.event_type,
      topic: row.topic,
      timestamp: new Date(row.created_at).toISOString(),
    }));
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
