# Event-Driven Architecture

## Overview

A production-style event-driven pipeline built with:

- **Kafka** for event streaming
- **Node.js + TypeScript** for the API and consumers
- **PostgreSQL** for persistent storage

## Data flow (linear pipeline)

Each stage emits to the topic consumed by the next stage:

```
POST /orders
  → OrderService saves the order (status: pending) and emits order.created → topic "orders"
      → PaymentConsumer (group payment-processor-group) consumes "orders"
          processes payment, status → payment_approved, emits payment.approved → topic "payments"
      → InventoryConsumer (group inventory-processor-group) consumes "payments"
          reserves inventory, status → inventory_reserved, emits inventory.reserved → topic "inventory"
      → NotificationConsumer (group notification-processor-group) consumes "inventory"
          sends confirmation, status → completed
```

Every transition is also written to the `order_events` table, so
`GET /orders/:id` can return the order's `events[]` timeline.

## Topics

| Topic | Partitions | Produced by | Consumed by |
|-------|-----------|-------------|-------------|
| orders | 3 | OrderService | PaymentConsumer |
| payments | 3 | PaymentConsumer | InventoryConsumer |
| inventory | 3 | InventoryConsumer | NotificationConsumer |
| notifications | 1 | (reserved for outbound notifications) | — |
| dlq | 1 | any consumer after max retries | manual review |

Partition key: `order_id` → the same order always maps to the same partition,
preserving per-order ordering.

## HTTP API

| Method | Path | Purpose |
|--------|------|---------|
| POST | /orders | Create an order (202 Accepted) |
| GET | /orders | List recent orders |
| GET | /orders/:id | Order detail incl. `events[]` timeline |
| GET | /health | DB + per-consumer health |
| GET | /metrics | Aggregate order metrics |

## Reliability

- **Idempotency** — each event is recorded in `processed_events`; duplicates are skipped.
- **Retries + DLQ** — a failing event is retried up to 3 times, then published to the `dlq` topic. If the DLQ publish itself fails, the source offset is **not** committed, so the event is redelivered rather than lost.
- **Order metadata** — items and status-transition metadata (`payment_id`, `reservation_id`) are persisted in the `orders.metadata` JSONB column using `COALESCE(metadata, '{}'::jsonb) || $2` so nothing is dropped on a NULL metadata.

## Configuration

- `DATABASE_URL` (preferred) or discrete `DB_*` variables.
- `KAFKA_BROKER` (comma-separated brokers).
- `LOG_LEVEL`, `PORT`, `NODE_ENV`.
