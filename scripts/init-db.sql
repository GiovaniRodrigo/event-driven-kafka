-- Registry of Kafka topics used by the pipeline (informational).
-- Application tables (orders, order_events, processed_events, metrics) are
-- created idempotently by the app on startup (DatabaseService.initialize).
CREATE TABLE IF NOT EXISTS kafka_topics (
  name VARCHAR(100) PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO kafka_topics (name) VALUES
  ('orders'),
  ('payments'),
  ('inventory'),
  ('notifications'),
  ('dlq')
ON CONFLICT DO NOTHING;
