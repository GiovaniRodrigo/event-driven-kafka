import { Pool, PoolClient } from 'pg';
import { databaseConfig } from '../config';
import { Order, OrderEvent } from '../types';
import { logger } from '../utils/logger';
import { EventEnvelope } from '../contracts/envelope';

export interface OutboxEventRow {
  id: string;
  aggregate_id: string;
  aggregate_type: string;
  event_type: string;
  event_version: number;
  payload: Record<string, unknown>;
  correlation_id: string;
  causation_id: string;
  topic: string;
  status: 'PENDING' | 'PROCESSING' | 'PUBLISHED' | 'FAILED';
  attempts: number;
  last_error?: string;
  lease_owner?: string;
  leased_at?: Date;
  lease_expires_at?: Date;
  created_at: Date;
  published_at?: Date;
}

export interface SagaInstanceRow {
  saga_id: string;
  aggregate_id: string;
  saga_type: string;
  state: string;
  current_step: string;
  correlation_id: string;
  context: Record<string, unknown>;
  failure_reason?: string;
  created_at: Date;
  updated_at: Date;
}

export interface DLQMessageRow {
  id: string;
  event_id: string;
  topic: string;
  partition: number;
  offset: string;
  consumer_name: string;
  error_message: string;
  stack_trace?: string;
  payload: Record<string, unknown>;
  correlation_id?: string;
  attempts: number;
  status: 'UNRESOLVED' | 'REPLAYING' | 'REPLAYED' | 'DISCARDED';
  failed_at: Date;
  resolved_at?: Date;
}

export class DatabaseService {
  private pool: Pool;

  constructor(poolOrConfig: any = databaseConfig) {
    if (poolOrConfig && typeof poolOrConfig.query === 'function' && typeof poolOrConfig.connect === 'function') {
      this.pool = poolOrConfig as Pool;
    } else {
      this.pool = new Pool(poolOrConfig);
    }

    this.pool.on('error', (error) => {
      logger.error({ event: 'pool_error', error: error.message });
    });
  }

  getPool(): Pool {
    return this.pool;
  }

  async initializeTables(): Promise<void> {
    await this.createTables();
  }

  async withTransaction<T>(callback: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
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

  async createTables() {
    const client = await this.pool.connect();

    try {
      // 1. Core Orders (Write Model)
      await client.query(`
        CREATE TABLE IF NOT EXISTS orders (
          id VARCHAR(100) PRIMARY KEY,
          user_id VARCHAR(100) NOT NULL,
          status VARCHAR(50) NOT NULL DEFAULT 'pending',
          total_amount DECIMAL(10, 2) NOT NULL,
          metadata JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
      `);

      // 2. Transactional Outbox Table with Explicit Leases
      await client.query(`
        CREATE TABLE IF NOT EXISTS outbox_events (
          id VARCHAR(100) PRIMARY KEY,
          aggregate_id VARCHAR(100) NOT NULL,
          aggregate_type VARCHAR(50) NOT NULL,
          event_type VARCHAR(100) NOT NULL,
          event_version INT NOT NULL DEFAULT 1,
          payload JSONB NOT NULL,
          correlation_id VARCHAR(100) NOT NULL,
          causation_id VARCHAR(100) NOT NULL,
          topic VARCHAR(100) NOT NULL,
          status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
          attempts INT NOT NULL DEFAULT 0,
          last_error TEXT,
          lease_owner VARCHAR(100),
          leased_at TIMESTAMP,
          lease_expires_at TIMESTAMP,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          published_at TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_outbox_pending
        ON outbox_events (status, created_at);
        CREATE INDEX IF NOT EXISTS idx_outbox_lease
        ON outbox_events (lease_expires_at)
        WHERE status = 'PROCESSING';
      `);

      // 3. Robust Idempotent Consumer Processed Events Table
      await client.query(`
        CREATE TABLE IF NOT EXISTS processed_events (
          id SERIAL PRIMARY KEY,
          event_id VARCHAR(100) NOT NULL,
          consumer_name VARCHAR(100) NOT NULL,
          status VARCHAR(50) NOT NULL DEFAULT 'PROCESSED',
          attempts INT NOT NULL DEFAULT 1,
          error TEXT,
          processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT uq_event_consumer UNIQUE (event_id, consumer_name)
        );
        CREATE INDEX IF NOT EXISTS idx_processed_lookup
        ON processed_events (event_id, consumer_name);
      `);

      // 4. Persistent Saga Instances Table
      await client.query(`
        CREATE TABLE IF NOT EXISTS saga_instances (
          saga_id VARCHAR(100) PRIMARY KEY,
          aggregate_id VARCHAR(100) NOT NULL,
          saga_type VARCHAR(100) NOT NULL,
          state VARCHAR(50) NOT NULL,
          current_step VARCHAR(50) NOT NULL,
          correlation_id VARCHAR(100) NOT NULL,
          context JSONB NOT NULL DEFAULT '{}'::jsonb,
          failure_reason TEXT,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_saga_aggregate ON saga_instances (aggregate_id);
        CREATE INDEX IF NOT EXISTS idx_saga_state ON saga_instances (state);
      `);

      // 5. Immutable Event Store with Explicit Sequence Ordering
      await client.query(`
        CREATE TABLE IF NOT EXISTS event_store (
          id SERIAL PRIMARY KEY,
          event_id VARCHAR(100) UNIQUE NOT NULL,
          aggregate_id VARCHAR(100) NOT NULL,
          aggregate_type VARCHAR(50) NOT NULL,
          event_type VARCHAR(100) NOT NULL,
          event_version INT NOT NULL DEFAULT 1,
          sequence_number INT NOT NULL DEFAULT 1,
          payload JSONB NOT NULL,
          correlation_id VARCHAR(100) NOT NULL,
          causation_id VARCHAR(100) NOT NULL,
          producer VARCHAR(100) NOT NULL,
          occurred_at TIMESTAMP NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT uq_event_store_aggregate_seq UNIQUE (aggregate_id, sequence_number)
        );
        CREATE INDEX IF NOT EXISTS idx_event_store_aggregate ON event_store (aggregate_id, sequence_number);
        CREATE INDEX IF NOT EXISTS idx_event_store_correlation ON event_store (correlation_id);

        -- Strict immutability protection trigger: forbids UPDATE and DELETE on event_store
        CREATE OR REPLACE FUNCTION prevent_event_store_mutation()
        RETURNS TRIGGER AS $$
        BEGIN
          RAISE EXCEPTION 'event_store is immutable and append-only. UPDATE and DELETE operations are forbidden.';
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS trg_prevent_event_store_mutation ON event_store;
        CREATE TRIGGER trg_prevent_event_store_mutation
        BEFORE UPDATE OR DELETE ON event_store
        FOR EACH ROW EXECUTE FUNCTION prevent_event_store_mutation();
      `);

      // 6. Projection Idempotency Log (Prevents double incremental mutations)
      await client.query(`
        CREATE TABLE IF NOT EXISTS projection_applied_events (
          projection_name VARCHAR(100) NOT NULL,
          event_id VARCHAR(100) NOT NULL,
          aggregate_id VARCHAR(100) NOT NULL,
          applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (projection_name, event_id)
        );
        CREATE INDEX IF NOT EXISTS idx_proj_applied_agg ON projection_applied_events (aggregate_id);
      `);

      // 7. CQRS Read Models
      await client.query(`
        CREATE TABLE IF NOT EXISTS order_read_model (
          order_id VARCHAR(100) PRIMARY KEY,
          user_id VARCHAR(100) NOT NULL,
          status VARCHAR(50) NOT NULL,
          total_amount DECIMAL(10, 2) NOT NULL,
          currency VARCHAR(10) NOT NULL DEFAULT 'USD',
          items JSONB NOT NULL DEFAULT '[]'::jsonb,
          payment_id VARCHAR(100),
          reservation_id VARCHAR(100),
          fraud_check_id VARCHAR(100),
          shipment_id VARCHAR(100),
          tracking_number VARCHAR(100),
          failure_reason TEXT,
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL,
          version INT NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS payment_read_model (
          payment_id VARCHAR(100) PRIMARY KEY,
          order_id VARCHAR(100) NOT NULL,
          user_id VARCHAR(100) NOT NULL,
          amount DECIMAL(10, 2) NOT NULL,
          status VARCHAR(50) NOT NULL,
          authorization_code VARCHAR(100),
          refund_id VARCHAR(100),
          failure_reason TEXT,
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL
        );

        CREATE TABLE IF NOT EXISTS inventory_read_model (
          sku VARCHAR(100) PRIMARY KEY,
          name VARCHAR(200) NOT NULL,
          total_stock INT NOT NULL DEFAULT 1000,
          reserved_stock INT NOT NULL DEFAULT 0,
          available_stock INT NOT NULL DEFAULT 1000,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS shipment_read_model (
          shipment_id VARCHAR(100) PRIMARY KEY,
          order_id VARCHAR(100) NOT NULL,
          tracking_number VARCHAR(100) NOT NULL,
          carrier VARCHAR(50) NOT NULL,
          status VARCHAR(50) NOT NULL,
          estimated_delivery TIMESTAMP,
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL
        );
      `);

      // Seed default inventory items if empty
      await client.query(`
        INSERT INTO inventory_read_model (sku, name, total_stock, reserved_stock, available_stock)
        VALUES 
          ('LAPTOP-001', 'High Performance Workstation', 100, 0, 100),
          ('PHONE-002', 'Smartphone Pro Max', 200, 0, 200),
          ('KEYBOARD-003', 'Wireless Mechanical Keyboard', 500, 0, 500),
          ('MONITOR-004', '4K Ultra-wide Monitor', 50, 0, 50),
          ('OUT_OF_STOCK_ITEM', 'Limited Edition Collectible', 0, 0, 0)
        ON CONFLICT (sku) DO NOTHING;
      `);

      // 8. Dead Letter Queue Table
      await client.query(`
        CREATE TABLE IF NOT EXISTS dlq_messages (
          id VARCHAR(100) PRIMARY KEY,
          event_id VARCHAR(100) NOT NULL,
          topic VARCHAR(100) NOT NULL,
          partition INT NOT NULL,
          offset_val VARCHAR(50) NOT NULL,
          consumer_name VARCHAR(100) NOT NULL,
          error_message TEXT NOT NULL,
          stack_trace TEXT,
          payload JSONB NOT NULL,
          correlation_id VARCHAR(100),
          attempts INT NOT NULL DEFAULT 1,
          status VARCHAR(50) NOT NULL DEFAULT 'UNRESOLVED',
          failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          resolved_at TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_dlq_status ON dlq_messages (status);
      `);

      // 9. Legacy / Timeline Order Events compatibility table
      await client.query(`
        CREATE TABLE IF NOT EXISTS order_events (
          id SERIAL PRIMARY KEY,
          order_id VARCHAR(100) NOT NULL,
          event_type VARCHAR(100) NOT NULL,
          topic VARCHAR(100) NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events (order_id, created_at);
      `);

      // 10. Metrics Table
      await client.query(`
        CREATE TABLE IF NOT EXISTS metrics (
          id SERIAL PRIMARY KEY,
          metric_name VARCHAR(100) NOT NULL,
          value DECIMAL(15, 2) NOT NULL,
          labels JSONB DEFAULT '{}'::jsonb,
          timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
      `);

      // 11. Transactional DLQ Outbox Table (Durable Crash Consistency)
      await client.query(`
        CREATE TABLE IF NOT EXISTS dlq_outbox (
          id VARCHAR(100) PRIMARY KEY,
          dlq_id VARCHAR(100) NOT NULL,
          topic VARCHAR(100) NOT NULL,
          payload JSONB NOT NULL,
          status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
          attempts INT NOT NULL DEFAULT 0,
          last_error TEXT,
          lease_owner VARCHAR(100),
          leased_at TIMESTAMP,
          lease_expires_at TIMESTAMP,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          published_at TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_dlq_outbox_pending ON dlq_outbox (status, created_at);
        CREATE INDEX IF NOT EXISTS idx_dlq_outbox_lease ON dlq_outbox (lease_expires_at) WHERE status = 'PROCESSING';
      `);

      logger.info({ event: 'database_tables_initialized' });
    } finally {
      client.release();
    }
  }

  // --- OUTBOX METHODS WITH LEASES ---

  async insertOutboxEvent(
    event: {
      id: string;
      aggregateId: string;
      aggregateType: string;
      eventType: string;
      eventVersion?: number;
      payload: Record<string, unknown>;
      correlationId: string;
      causationId: string;
      topic: string;
    },
    client?: PoolClient
  ): Promise<void> {
    const query = `
      INSERT INTO outbox_events (
        id, aggregate_id, aggregate_type, event_type, event_version,
        payload, correlation_id, causation_id, topic, status, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING', CURRENT_TIMESTAMP)
      ON CONFLICT (id) DO NOTHING
    `;
    const params = [
      event.id,
      event.aggregateId,
      event.aggregateType,
      event.eventType,
      event.eventVersion || 1,
      JSON.stringify(event.payload),
      event.correlationId,
      event.causationId,
      event.topic,
    ];

    if (client) {
      await client.query(query, params);
    } else {
      await this.pool.query(query, params);
    }
  }

  async getPendingOutboxEvents(
    limit = 50,
    workerId = 'default-worker',
    leaseDurationSeconds = 30,
    client?: PoolClient
  ): Promise<OutboxEventRow[]> {
    // Atomic lease acquisition: claims pending/expired events and marks them PROCESSING with worker lease
    const query = `
      UPDATE outbox_events
      SET status = 'PROCESSING',
          lease_owner = $2,
          leased_at = CURRENT_TIMESTAMP,
          lease_expires_at = CURRENT_TIMESTAMP + ($3 || ' seconds')::INTERVAL,
          attempts = attempts + 1
      WHERE id IN (
        SELECT id FROM outbox_events
        WHERE (status = 'PENDING' OR (status = 'PROCESSING' AND (lease_expires_at IS NULL OR lease_expires_at < CURRENT_TIMESTAMP)))
          AND attempts < 10
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *;
    `;
    const runner = client || this.pool;
    const result = await runner.query(query, [limit, workerId, leaseDurationSeconds]);
    return result.rows.map((r) => ({
      id: r.id,
      aggregate_id: r.aggregate_id,
      aggregate_type: r.aggregate_type,
      event_type: r.event_type,
      event_version: r.event_version,
      payload: r.payload,
      correlation_id: r.correlation_id,
      causation_id: r.causation_id,
      topic: r.topic,
      status: r.status,
      attempts: r.attempts,
      last_error: r.last_error,
      lease_owner: r.lease_owner,
      leased_at: r.leased_at ? new Date(r.leased_at) : undefined,
      lease_expires_at: r.lease_expires_at ? new Date(r.lease_expires_at) : undefined,
      created_at: new Date(r.created_at),
      published_at: r.published_at ? new Date(r.published_at) : undefined,
    }));
  }

  async markOutboxEventPublished(id: string, workerId?: string, client?: PoolClient): Promise<boolean> {
    const query = `
      UPDATE outbox_events
      SET status = 'PUBLISHED',
          published_at = CURRENT_TIMESTAMP,
          lease_owner = NULL,
          lease_expires_at = NULL
      WHERE id = $1
        AND status = 'PROCESSING'
        AND ($2::VARCHAR IS NULL OR lease_owner = $2)
    `;
    const runner = client || this.pool;
    const result = await runner.query(query, [id, workerId || null]);
    const success = (result.rowCount ?? 0) > 0;
    if (!success && workerId) {
      logger.warn({
        event: 'outbox.stale_worker_rejected',
        event_id: id,
        worker_id: workerId,
        action: 'MARK_PUBLISHED',
        reason: 'STALE_WORKER_LOST_LEASE',
      });
    }
    return success;
  }

  async markOutboxEventFailed(id: string, error: string, workerId?: string, client?: PoolClient): Promise<boolean> {
    const query = `
      UPDATE outbox_events
      SET last_error = $2,
          lease_owner = NULL,
          lease_expires_at = NULL,
          status = CASE WHEN attempts >= 10 THEN 'FAILED' ELSE 'PENDING' END
      WHERE id = $1
        AND status = 'PROCESSING'
        AND ($3::VARCHAR IS NULL OR lease_owner = $3)
    `;
    const runner = client || this.pool;
    const result = await runner.query(query, [id, error, workerId || null]);
    const success = (result.rowCount ?? 0) > 0;
    if (!success && workerId) {
      logger.warn({
        event: 'outbox.stale_worker_rejected',
        event_id: id,
        worker_id: workerId,
        action: 'MARK_FAILED',
        reason: 'STALE_WORKER_LOST_LEASE',
      });
    }
    return success;
  }

  // --- DLQ OUTBOX METHODS (TRANSACTIONAL CRASH CONSISTENCY) ---

  async insertDLQOutboxEvent(
    event: {
      id: string;
      dlqId: string;
      topic: string;
      payload: Record<string, unknown>;
    },
    client?: PoolClient
  ): Promise<void> {
    const query = `
      INSERT INTO dlq_outbox (
        id, dlq_id, topic, payload, status, created_at
      ) VALUES ($1, $2, $3, $4, 'PENDING', CURRENT_TIMESTAMP)
      ON CONFLICT (id) DO NOTHING
    `;
    const runner = client || this.pool;
    await runner.query(query, [event.id, event.dlqId, event.topic, JSON.stringify(event.payload)]);
  }

  async getPendingDLQOutboxEvents(
    limit = 50,
    workerId = 'default-dlq-worker',
    leaseDurationSeconds = 30,
    client?: PoolClient
  ): Promise<Array<{
    id: string;
    dlq_id: string;
    topic: string;
    payload: any;
    status: string;
    attempts: number;
    last_error?: string;
    lease_owner?: string;
    leased_at?: Date;
    lease_expires_at?: Date;
    created_at: Date;
    published_at?: Date;
  }>> {
    const query = `
      UPDATE dlq_outbox
      SET status = 'PROCESSING',
          lease_owner = $2,
          leased_at = CURRENT_TIMESTAMP,
          lease_expires_at = CURRENT_TIMESTAMP + ($3 || ' seconds')::INTERVAL,
          attempts = attempts + 1
      WHERE id IN (
        SELECT id FROM dlq_outbox
        WHERE (status = 'PENDING' OR (status = 'PROCESSING' AND (lease_expires_at IS NULL OR lease_expires_at < CURRENT_TIMESTAMP)))
          AND attempts < 10
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *;
    `;
    const runner = client || this.pool;
    const result = await runner.query(query, [limit, workerId, leaseDurationSeconds]);
    return result.rows.map((r) => ({
      id: r.id,
      dlq_id: r.dlq_id,
      topic: r.topic,
      payload: r.payload,
      status: r.status,
      attempts: r.attempts,
      last_error: r.last_error,
      lease_owner: r.lease_owner,
      leased_at: r.leased_at ? new Date(r.leased_at) : undefined,
      lease_expires_at: r.lease_expires_at ? new Date(r.lease_expires_at) : undefined,
      created_at: new Date(r.created_at),
      published_at: r.published_at ? new Date(r.published_at) : undefined,
    }));
  }

  async markDLQOutboxPublished(id: string, workerId?: string, client?: PoolClient): Promise<boolean> {
    const query = `
      UPDATE dlq_outbox
      SET status = 'PUBLISHED',
          published_at = CURRENT_TIMESTAMP,
          lease_owner = NULL,
          lease_expires_at = NULL
      WHERE id = $1
        AND status = 'PROCESSING'
        AND ($2::VARCHAR IS NULL OR lease_owner = $2)
    `;
    const runner = client || this.pool;
    const result = await runner.query(query, [id, workerId || null]);
    return (result.rowCount ?? 0) > 0;
  }

  async markDLQOutboxFailed(id: string, error: string, workerId?: string, client?: PoolClient): Promise<boolean> {
    const query = `
      UPDATE dlq_outbox
      SET last_error = $2,
          lease_owner = NULL,
          lease_expires_at = NULL,
          status = CASE WHEN attempts >= 10 THEN 'FAILED' ELSE 'PENDING' END
      WHERE id = $1
        AND status = 'PROCESSING'
        AND ($3::VARCHAR IS NULL OR lease_owner = $3)
    `;
    const runner = client || this.pool;
    const result = await runner.query(query, [id, error, workerId || null]);
    return (result.rowCount ?? 0) > 0;
  }

  // --- IDEMPOTENCY / PROCESSED EVENTS ---

  async isEventProcessed(eventId: string, consumerName: string): Promise<boolean> {
    const query = `
      SELECT 1 FROM processed_events
      WHERE event_id = $1 AND consumer_name = $2 AND status = 'PROCESSED'
    `;
    const result = await this.pool.query(query, [eventId, consumerName]);
    return result.rows.length > 0;
  }

  async resetProcessedEventForReplay(eventId: string, consumerName?: string): Promise<void> {
    if (consumerName) {
      await this.pool.query('DELETE FROM processed_events WHERE event_id = $1 AND consumer_name = $2', [eventId, consumerName]);
    } else {
      await this.pool.query('DELETE FROM processed_events WHERE event_id = $1', [eventId]);
    }
  }

  /** Legacy helper for backward compatibility */
  async getProcessedEvent(eventId: string): Promise<boolean> {
    const query = "SELECT 1 FROM processed_events WHERE event_id = $1 AND status = 'PROCESSED'";
    const result = await this.pool.query(query, [eventId]);
    return result.rows.length > 0;
  }

  async markEventProcessed(
    eventId: string,
    consumerName: string,
    status = 'PROCESSED',
    error?: string,
    client?: PoolClient
  ): Promise<void> {
    const query = `
      INSERT INTO processed_events (event_id, consumer_name, status, error, processed_at)
      VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
      ON CONFLICT (event_id, consumer_name) DO UPDATE
      SET status = EXCLUDED.status, error = EXCLUDED.error, processed_at = CURRENT_TIMESTAMP
    `;
    const runner = client || this.pool;
    await runner.query(query, [eventId, consumerName, status, error || null]);
  }

  /** Legacy helper for backward compatibility */
  async markProcessed(eventId: string): Promise<void> {
    await this.markEventProcessed(eventId, 'default-consumer');
  }

  // --- PROJECTION IDEMPOTENCY LOG ---

  async isProjectionEventApplied(projectionName: string, eventId: string, client?: PoolClient): Promise<boolean> {
    const query = `
      SELECT 1 FROM projection_applied_events
      WHERE projection_name = $1 AND event_id = $2
    `;
    const runner = client || this.pool;
    const result = await runner.query(query, [projectionName, eventId]);
    return result.rows.length > 0;
  }

  async markProjectionEventApplied(
    projectionName: string,
    eventId: string,
    aggregateId: string,
    client?: PoolClient
  ): Promise<void> {
    const query = `
      INSERT INTO projection_applied_events (projection_name, event_id, aggregate_id, applied_at)
      VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
      ON CONFLICT (projection_name, event_id) DO NOTHING
    `;
    const runner = client || this.pool;
    await runner.query(query, [projectionName, eventId, aggregateId]);
  }

  async resetReadModelForAggregate(aggregateId: string, client?: PoolClient): Promise<void> {
    const runner = client || this.pool;
    await runner.query('DELETE FROM order_read_model WHERE order_id = $1', [aggregateId]);
    await runner.query('DELETE FROM payment_read_model WHERE order_id = $1', [aggregateId]);
    await runner.query('DELETE FROM shipment_read_model WHERE order_id = $1', [aggregateId]);
    await runner.query('DELETE FROM projection_applied_events WHERE aggregate_id = $1', [aggregateId]);
  }

  // --- SAGA INSTANCE METHODS ---

  async saveSagaInstance(
    saga: {
      sagaId: string;
      aggregateId: string;
      sagaType: string;
      state: string;
      currentStep: string;
      correlationId: string;
      context: Record<string, unknown>;
      failureReason?: string;
    },
    client?: PoolClient
  ): Promise<void> {
    const query = `
      INSERT INTO saga_instances (
        saga_id, aggregate_id, saga_type, state, current_step,
        correlation_id, context, failure_reason, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ON CONFLICT (saga_id) DO UPDATE
      SET state = EXCLUDED.state,
          current_step = EXCLUDED.current_step,
          context = EXCLUDED.context,
          failure_reason = EXCLUDED.failure_reason,
          updated_at = CURRENT_TIMESTAMP
    `;
    const runner = client || this.pool;
    await runner.query(query, [
      saga.sagaId,
      saga.aggregateId,
      saga.sagaType,
      saga.state,
      saga.currentStep,
      saga.correlationId,
      JSON.stringify(saga.context),
      saga.failureReason || null,
    ]);
  }

  async getSagaInstance(sagaId: string, client?: PoolClient): Promise<SagaInstanceRow | null> {
    const query = 'SELECT * FROM saga_instances WHERE saga_id = $1';
    const runner = client || this.pool;
    const result = await runner.query(query, [sagaId]);
    if (result.rows.length === 0) return null;
    const r = result.rows[0];
    return {
      saga_id: r.saga_id,
      aggregate_id: r.aggregate_id,
      saga_type: r.saga_type,
      state: r.state,
      current_step: r.current_step,
      correlation_id: r.correlation_id,
      context: r.context,
      failure_reason: r.failure_reason,
      created_at: new Date(r.created_at),
      updated_at: new Date(r.updated_at),
    };
  }

  async getSagaByAggregateId(aggregateId: string, client?: PoolClient): Promise<SagaInstanceRow | null> {
    const query = 'SELECT * FROM saga_instances WHERE aggregate_id = $1 ORDER BY created_at DESC LIMIT 1';
    const runner = client || this.pool;
    const result = await runner.query(query, [aggregateId]);
    if (result.rows.length === 0) return null;
    const r = result.rows[0];
    return {
      saga_id: r.saga_id,
      aggregate_id: r.aggregate_id,
      saga_type: r.saga_type,
      state: r.state,
      current_step: r.current_step,
      correlation_id: r.correlation_id,
      context: r.context,
      failure_reason: r.failure_reason,
      created_at: new Date(r.created_at),
      updated_at: new Date(r.updated_at),
    };
  }

  async getSagaByAggregateIdForUpdate(aggregateId: string, client: PoolClient): Promise<SagaInstanceRow | null> {
    const query = 'SELECT * FROM saga_instances WHERE aggregate_id = $1 ORDER BY created_at DESC LIMIT 1 FOR UPDATE';
    const result = await client.query(query, [aggregateId]);
    if (result.rows.length === 0) return null;
    const r = result.rows[0];
    return {
      saga_id: r.saga_id,
      aggregate_id: r.aggregate_id,
      saga_type: r.saga_type,
      state: r.state,
      current_step: r.current_step,
      correlation_id: r.correlation_id,
      context: r.context,
      failure_reason: r.failure_reason,
      created_at: new Date(r.created_at),
      updated_at: new Date(r.updated_at),
    };
  }

  async listSagas(limit = 50): Promise<SagaInstanceRow[]> {
    const query = 'SELECT * FROM saga_instances ORDER BY created_at DESC LIMIT $1';
    const result = await this.pool.query(query, [limit]);
    return result.rows.map((r) => ({
      saga_id: r.saga_id,
      aggregate_id: r.aggregate_id,
      saga_type: r.saga_type,
      state: r.state,
      current_step: r.current_step,
      correlation_id: r.correlation_id,
      context: r.context,
      failure_reason: r.failure_reason,
      created_at: new Date(r.created_at),
      updated_at: new Date(r.updated_at),
    }));
  }

  // --- EVENT STORE METHODS WITH EXPLICIT SEQUENCE ORDERING ---

  async appendToEventStore(event: EventEnvelope, client?: PoolClient): Promise<void> {
    const execute = async (c: PoolClient) => {
      // Serialize appends per aggregate to prevent sequence race conditions
      try {
        await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', [event.aggregate_id]);
      } catch {
        // Fallback if advisory lock unavailable
      }
      const query = `
        INSERT INTO event_store (
          event_id, aggregate_id, aggregate_type, event_type, event_version,
          sequence_number, payload, correlation_id, causation_id, producer, occurred_at, created_at
        ) VALUES (
          $1, $2, $3, $4, $5,
          COALESCE($6::INT, (SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM event_store WHERE aggregate_id = $2)),
          $7, $8, $9, $10, $11::TIMESTAMP, CURRENT_TIMESTAMP
        )
        ON CONFLICT (event_id) DO NOTHING
      `;
      const params = [
        event.event_id,
        event.aggregate_id,
        event.aggregate_type,
        event.event_type,
        event.event_version,
        event.sequence_number || null,
        JSON.stringify(event.payload),
        event.correlation_id,
        event.causation_id,
        event.producer,
        event.occurred_at,
      ];
      await c.query(query, params);
    };

    if (client) {
      await execute(client);
    } else {
      await this.withTransaction(async (c) => {
        await execute(c);
      });
    }
  }

  async getEventsByAggregateId(aggregateId: string): Promise<EventEnvelope[]> {
    const query = `
      SELECT * FROM event_store
      WHERE aggregate_id = $1
      ORDER BY sequence_number ASC, occurred_at ASC
    `;
    const result = await this.pool.query(query, [aggregateId]);
    return result.rows.map((r) => ({
      event_id: r.event_id,
      aggregate_id: r.aggregate_id,
      aggregate_type: r.aggregate_type,
      event_type: r.event_type,
      event_version: r.event_version,
      sequence_number: r.sequence_number,
      payload: r.payload,
      correlation_id: r.correlation_id,
      causation_id: r.causation_id,
      producer: r.producer,
      occurred_at: new Date(r.occurred_at).toISOString(),
      schema_version: 1,
    }));
  }

  // --- DLQ METHODS ---

  async recordDLQMessage(
    dlq: {
      id: string;
      eventId: string;
      topic: string;
      partition: number;
      offset: string;
      consumerName: string;
      errorMessage: string;
      stackTrace?: string;
      payload: Record<string, unknown>;
      correlationId?: string;
    },
    client?: PoolClient
  ): Promise<void> {
    const query = `
      INSERT INTO dlq_messages (
        id, event_id, topic, partition, offset_val, consumer_name,
        error_message, stack_trace, payload, correlation_id, status, failed_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'UNRESOLVED', CURRENT_TIMESTAMP)
      ON CONFLICT (id) DO UPDATE
      SET attempts = dlq_messages.attempts + 1,
          error_message = EXCLUDED.error_message,
          failed_at = CURRENT_TIMESTAMP
    `;
    const runner = client || this.pool;
    await runner.query(query, [
      dlq.id,
      dlq.eventId,
      dlq.topic,
      dlq.partition,
      dlq.offset,
      dlq.consumerName,
      dlq.errorMessage,
      dlq.stackTrace || null,
      JSON.stringify(dlq.payload),
      dlq.correlationId || null,
    ]);
  }

  async setDLQStatus(id: string, status: 'UNRESOLVED' | 'REPLAYING' | 'REPLAYED' | 'DISCARDED'): Promise<void> {
    const query = `
      UPDATE dlq_messages
      SET status = $2,
          resolved_at = CASE WHEN $2 IN ('REPLAYED', 'DISCARDED') THEN CURRENT_TIMESTAMP ELSE resolved_at END
      WHERE id = $1
    `;
    await this.pool.query(query, [id, status]);
  }

  async listDLQMessages(limit = 50, status = 'UNRESOLVED'): Promise<DLQMessageRow[]> {
    const query = `
      SELECT * FROM dlq_messages
      WHERE status = $1
      ORDER BY failed_at DESC
      LIMIT $2
    `;
    const result = await this.pool.query(query, [status, limit]);
    return result.rows.map((r) => ({
      id: r.id,
      event_id: r.event_id,
      topic: r.topic,
      partition: r.partition,
      offset: r.offset_val,
      consumer_name: r.consumer_name,
      error_message: r.error_message,
      stack_trace: r.stack_trace,
      payload: r.payload,
      correlation_id: r.correlation_id,
      attempts: r.attempts,
      status: r.status,
      failed_at: new Date(r.failed_at),
      resolved_at: r.resolved_at ? new Date(r.resolved_at) : undefined,
    }));
  }

  async getDLQMessage(id: string): Promise<DLQMessageRow | null> {
    const query = 'SELECT * FROM dlq_messages WHERE id = $1';
    const result = await this.pool.query(query, [id]);
    if (result.rows.length === 0) return null;
    const r = result.rows[0];
    return {
      id: r.id,
      event_id: r.event_id,
      topic: r.topic,
      partition: r.partition,
      offset: r.offset_val,
      consumer_name: r.consumer_name,
      error_message: r.error_message,
      stack_trace: r.stack_trace,
      payload: r.payload,
      correlation_id: r.correlation_id,
      attempts: r.attempts,
      status: r.status,
      failed_at: new Date(r.failed_at),
      resolved_at: r.resolved_at ? new Date(r.resolved_at) : undefined,
    };
  }

  async markDLQResolved(id: string, status: 'REPLAYED' | 'DISCARDED' = 'REPLAYED'): Promise<void> {
    await this.setDLQStatus(id, status);
  }

  // --- ORDERS / READ MODEL METHODS ---

  async insertOrder(order: Order, client?: PoolClient): Promise<void> {
    const query = `
      INSERT INTO orders (id, user_id, status, total_amount, metadata, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (id) DO UPDATE
      SET status = EXCLUDED.status, metadata = EXCLUDED.metadata, updated_at = EXCLUDED.updated_at
    `;
    const runner = client || this.pool;
    await runner.query(query, [
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
    // Check read model first, fallback to orders
    const query = 'SELECT * FROM order_read_model WHERE order_id = $1';
    const result = await this.pool.query(query, [orderId]);
    if (result.rows.length > 0) {
      const row = result.rows[0];
      return {
        id: row.order_id,
        user_id: row.user_id,
        status: row.status,
        items: row.items || [],
        total_amount: parseFloat(row.total_amount),
        created_at: new Date(row.created_at),
        updated_at: new Date(row.updated_at),
      };
    }

    const legacyQuery = 'SELECT * FROM orders WHERE id = $1';
    const legacyResult = await this.pool.query(legacyQuery, [orderId]);
    if (legacyResult.rows.length === 0) return null;
    return this.mapOrderRow(legacyResult.rows[0]);
  }

  async listOrders(limit = 50): Promise<Order[]> {
    const query = 'SELECT * FROM order_read_model ORDER BY created_at DESC LIMIT $1';
    const result = await this.pool.query(query, [limit]);
    if (result.rows.length > 0) {
      return result.rows.map((row) => ({
        id: row.order_id,
        user_id: row.user_id,
        status: row.status,
        items: row.items || [],
        total_amount: parseFloat(row.total_amount),
        created_at: new Date(row.created_at),
        updated_at: new Date(row.updated_at),
      }));
    }

    const legacyQuery = 'SELECT * FROM orders ORDER BY created_at DESC LIMIT $1';
    const legacyResult = await this.pool.query(legacyQuery, [limit]);
    return legacyResult.rows.map((row) => this.mapOrderRow(row));
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
    // Fetch from event_store first
    const esQuery = `
      SELECT event_type, producer, occurred_at
      FROM event_store
      WHERE aggregate_id = $1
      ORDER BY sequence_number ASC, occurred_at ASC
    `;
    const esResult = await this.pool.query(esQuery, [orderId]);
    if (esResult.rows.length > 0) {
      return esResult.rows.map((row) => ({
        event_type: row.event_type,
        topic: row.producer,
        timestamp: new Date(row.occurred_at).toISOString(),
      }));
    }

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

  async getMetrics() {
    const query = `
      SELECT
        COUNT(*) as total_orders,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
        COUNT(CASE WHEN status = 'failed' OR status = 'cancelled' THEN 1 END) as failed_orders,
        COALESCE(AVG(total_amount), 0) as avg_order_value
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
