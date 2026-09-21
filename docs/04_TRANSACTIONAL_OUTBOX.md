# 04. Transactional Outbox Pattern & Relay Engine

**Version:** 1.2.0  
**Author:** Giovani Rodrigo  
**Status:** PRODUCTION HARDENED & LEASE FENCED  

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

    loop Asynchronous Polling with Atomic Lease & Fencing
        Relay->>DB: UPDATE outbox_events SET status = 'PROCESSING', lease_owner = $worker, lease_expires_at = NOW() + INTERVAL '30s' WHERE id IN (SELECT id ... FOR UPDATE SKIP LOCKED) RETURNING *
        Relay->>Kafka: Produce event to topic (Key = aggregate_id)
        Kafka-->>Relay: Ack (Partition, Offset)
        Relay->>DB: UPDATE outbox_events SET status = 'PUBLISHED' WHERE id = $1 AND status = 'PROCESSING' AND lease_owner = $worker
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
  lease_owner VARCHAR(100),
  leased_at TIMESTAMP,
  lease_expires_at TIMESTAMP,
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  published_at TIMESTAMP
);

CREATE INDEX idx_outbox_pending
ON outbox_events (status, created_at);

CREATE INDEX idx_outbox_lease_recovery
ON outbox_events (status, lease_expires_at)
WHERE status = 'PROCESSING';
```

---

## 4. Outbox Relay Concurrency & Guarantees

### 4.1 Non-Blocking Concurrency (`FOR UPDATE SKIP LOCKED`)
Multiple outbox relay worker processes or threads can safely run concurrently. By executing `FOR UPDATE SKIP LOCKED`, each worker locks a unique slice of pending events without blocking or contending with peer workers.

### 4.2 Explicit Lease Ownership & Worker Crash Recovery
Each batch of rows is atomically assigned to a worker (`lease_owner = workerId`) with a deterministic lease expiration (`lease_expires_at = NOW() + INTERVAL '30 seconds'`). If a worker crashes mid-batch, subsequent relay ticks automatically claim expired rows (`lease_expires_at < NOW()`).

### 4.3 Stale Worker Lease Fencing (`STALE_WORKER_LOST_LEASE`)
If Worker A's network stalls and its lease expires, Worker B claims the row (`lease_owner = Worker B`). If Worker A later finishes and attempts to mark the row as `PUBLISHED`, the conditional SQL update:
```sql
UPDATE outbox_events
SET status = 'PUBLISHED', published_at = CURRENT_TIMESTAMP, lease_owner = NULL, lease_expires_at = NULL
WHERE id = $1 AND status = 'PROCESSING' AND lease_owner = $2
```
matches `0` rows. The database rejects Worker A's stale completion (`rowCount === 0`), and Worker B remains authoritative.

### 4.4 At-Least-Once Delivery Guarantee
If an outbox worker crashes after Kafka acknowledges the message but before updating PostgreSQL to `PUBLISHED`, the next polling iteration will redeliver the event. Downstream consumers eliminate duplicate deliveries via their **Scoped Idempotent Consumer Guards** (`UNIQUE(event_id, consumer_name)`).
