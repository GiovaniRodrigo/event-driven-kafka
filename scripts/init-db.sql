-- Complete schema initialization for Event-Driven Architecture Platform

-- 1. Topics Registry
CREATE TABLE IF NOT EXISTS kafka_topics (
  name VARCHAR(100) PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO kafka_topics (name) VALUES
  ('orders.events'),
  ('payments.events'),
  ('inventory.events'),
  ('fraud.events'),
  ('shipping.events'),
  ('notifications.events'),
  ('orders.retry'),
  ('payments.retry'),
  ('inventory.retry'),
  ('fraud.retry'),
  ('shipping.retry'),
  ('platform.dlq'),
  ('platform.events'),
  ('replay.events')
ON CONFLICT DO NOTHING;

-- 2. Orders Write Model
CREATE TABLE IF NOT EXISTS orders (
  id VARCHAR(100) PRIMARY KEY,
  user_id VARCHAR(100) NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'pending',
  total_amount DECIMAL(10, 2) NOT NULL,
  metadata JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Transactional Outbox with Leases
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
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (status, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_lease ON outbox_events (lease_expires_at) WHERE status = 'PROCESSING';

-- 4. Scoped Idempotency Consumer Tracking
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
CREATE INDEX IF NOT EXISTS idx_processed_lookup ON processed_events (event_id, consumer_name);

-- 5. Persistent Saga Instances
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

-- 6. Immutable Append-Only Event Store with Explicit Sequence Ordering
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

-- Enforce Immutability Trigger on event_store
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

-- 7. Projection Idempotency Log
CREATE TABLE IF NOT EXISTS projection_applied_events (
  projection_name VARCHAR(100) NOT NULL,
  event_id VARCHAR(100) NOT NULL,
  aggregate_id VARCHAR(100) NOT NULL,
  applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (projection_name, event_id)
);
CREATE INDEX IF NOT EXISTS idx_proj_applied_agg ON projection_applied_events (aggregate_id);

-- 8. CQRS Materialized Read Models
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

INSERT INTO inventory_read_model (sku, name, total_stock, reserved_stock, available_stock)
VALUES 
  ('LAPTOP-001', 'High Performance Workstation', 100, 0, 100),
  ('PHONE-002', 'Smartphone Pro Max', 200, 0, 200),
  ('KEYBOARD-003', 'Wireless Mechanical Keyboard', 500, 0, 500),
  ('MONITOR-004', '4K Ultra-wide Monitor', 50, 0, 50),
  ('OUT_OF_STOCK_ITEM', 'Limited Edition Collectible', 0, 0, 0)
ON CONFLICT (sku) DO NOTHING;

-- 9. Dead Letter Queue Table
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

-- 10. Metrics & Legacy Event Log
CREATE TABLE IF NOT EXISTS order_events (
  id SERIAL PRIMARY KEY,
  order_id VARCHAR(100) NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  topic VARCHAR(100) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events (order_id, created_at);

CREATE TABLE IF NOT EXISTS metrics (
  id SERIAL PRIMARY KEY,
  metric_name VARCHAR(100) NOT NULL,
  value DECIMAL(15, 2) NOT NULL,
  labels JSONB DEFAULT '{}'::jsonb,
  timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
