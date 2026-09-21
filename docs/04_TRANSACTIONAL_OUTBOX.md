# 04. Transactional Outbox Pattern & Relay Engine

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Problem Statement: The Dual-Write Hazard

In distributed systems, updating a local database and publishing a message to a broker as two distinct, uncoordinated steps introduces a critical consistency flaw:

```mermaid
flowchart TD
    A["HTTP POST /orders"] --> B["1. DB INSERT Order"]
    B -->|Success| C["2. Kafka Publish OrderCreated"]
    C -->|Network Failure / Node Crash| D["💥 Inconsistent State:\nOrder is in DB, but Kafka never saw the event!"]
```

If the database commit succeeds but the process terminates or Kafka fails before publishing, downstream services (Payment, Inventory, Shipping) are never notified. The order hangs forever in a zombie state.

---

## 2. Solution: The Transactional Outbox Pattern

The **Transactional Outbox Pattern** guarantees atomic state transition and event generation by co-locating the business state update and the domain event in the same local relational database transaction:

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as Order API
    participant DB as PostgreSQL
    participant Relay as Outbox Relay Engine
    participant Kafka as Kafka Broker

    Client->>API: POST /orders
    rect rgb(240, 248, 255)
        Note over API,DB: Single Atomic ACID Transaction
        API->>DB: BEGIN
        API->>DB: INSERT INTO orders (status = 'pending')
        API->>DB: INSERT INTO outbox_events (status = 'PENDING')
        API->>DB: COMMIT
    end
    API-->>Client: 202 Accepted (order_id, correlation_id)

    loop Asynchronous Polling / Trigger
        Relay->>DB: SELECT ... FROM outbox_events WHERE status = 'PENDING' FOR UPDATE SKIP LOCKED
        Relay->>Kafka: Produce event to topic (Key = aggregate_id)
        Kafka-->>Relay: Ack (Partition, Offset)
        Relay->>DB: UPDATE outbox_events SET status = 'PUBLISHED', published_at = NOW()
    end
```

---

## 3. Database Schema: `outbox_events`

```sql
CREATE TABLE outbox_events (
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
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  published_at TIMESTAMP
);

CREATE INDEX idx_outbox_pending
ON outbox_events (status, created_at)
WHERE status = 'PENDING';
```

---

## 4. Outbox Relay Concurrency & Guarantees

### 4.1 Non-Blocking Concurrency (`FOR UPDATE SKIP LOCKED`)
Multiple outbox relay worker processes or threads can safely run concurrently. By executing `FOR UPDATE SKIP LOCKED`, each worker locks a unique slice of pending events without blocking or contending with peer workers.

### 4.2 At-Least-Once Delivery Guarantee
If an outbox worker crashes after Kafka acknowledges the message but before updating PostgreSQL to `PUBLISHED`, the next polling iteration will redeliver the event. Downstream consumers eliminate duplicate deliveries via their **Idempotent Consumer Guards** (`processed_events`).
