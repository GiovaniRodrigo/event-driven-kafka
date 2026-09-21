# 20. Failure & Resilience Matrix

**Project:** Event-Driven Kafka Fulfillment Platform  
**Version:** 1.0.0  
**Author:** Giovani Rodrigo ([giovanif245@gmail.com](mailto:giovanif245@gmail.com))  
**Status:** PRODUCTION HARDENED & VERIFIED  

---

## 1. Executive Failure Architecture Overview

In an asynchronous, distributed event-driven system operating over Apache Kafka and PostgreSQL, failures are not anomalies; they are normal operational states. The platform enforces a zero-data-loss, self-healing, and partition-tolerant architecture governed by three core resilience pillars:

1. **At-Least-Once Messaging with Consumer-Scoped Idempotency:** Guarantees no lost events while completely neutralizing duplicate delivery side effects across distinct consumer groups (`UNIQUE(event_id, consumer_name)`).
2. **Transactional Outbox with Atomic Leases:** Eliminates dual-write anomalies between database state mutations and message broker publications using PostgreSQL `UPDATE ... FOR UPDATE SKIP LOCKED` state transitions.
3. **Orchestrated Saga Compensations with Strict Transition Guards:** Guarantees eventual consistency and clean rollbacks for distributed business failures (`PaymentRefundRequested`, `InventoryReleased`, `OrderCancelled`).

---

## 2. Comprehensive Distributed Failure Matrix

| ID | Failure Scenario | Bounded Context | Error Class | Trigger / Simulation | Detection Mechanism | Retry Policy | Dead Letter Queue | Compensating Action | Recovery & Operator Protocol | Idempotency & Consistency Guarantee |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **FM-01** | **Payment Gateway Decline (Card Expired / Insufficient Funds)** | `PaymentService` | Domain Rejection (Permanent) | User invalid card or `POST /chaos/payment/failure` | `PaymentService` emits `PaymentRejected` event. | 0 retries (Domain rejection is permanent). | None (Handled as normal business event). | Saga Orchestrator transitions to `FAILED`; order marked `FAILED`. | User is notified via `OrderFailed` event to update payment method. | Order remains in `FAILED` state; no funds captured; no inventory reserved. |
| **FM-02** | **Payment Gateway Network Timeout / Transient HTTP 503** | `PaymentService` | Transient Network | Network spike or `POST /chaos/latency { "ms": 5000 }` | Consumer execution timeout caught in `BaseConsumer.handleMessage`. | Exponential backoff (`500ms * 2^attempt + jitter`, max 3 attempts). | If 3 attempts exhausted, routed to `platform.dlq` + `dlq_messages` table. | If DLQ reached, saga remains in `PAYMENT_PENDING` until DLQ replayed or timed out. | Operator inspects `/dlq` endpoint; triggers `POST /dlq/:id/replay` after gateway stabilizes. | Offset committed only after DLQ quarantine; `processed_events` marked `FAILED` then reset on replay. |
| **FM-03** | **Inventory Shortage (Out of Stock / Stock Depleted)** | `InventoryService` | Domain Rejection (Permanent) | Order with `OUT_OF_STOCK_ITEM` or `POST /chaos/inventory/failure` | `InventoryService` verifies available stock and emits `InventoryReservationFailed`. | 0 retries (Stock shortage is not transient). | None (Handled by Saga compensation). | Saga enters `COMPENSATING`; emits `PaymentRefundRequested`; once refunded, emits `OrderCancelled`. | Automated compensation completes without human intervention. | Captured payment fully refunded via `PaymentRefunded`; order read model updated to `CANCELLED`. |
| **FM-04** | **High-Risk Fraud Rejection (Score > 80)** | `FraudService` | Domain Rejection (Permanent) | Order amount > $10,000 or `POST /chaos/fraud/rejection` | `FraudService` emits `FraudRejected` with risk breakdown. | 0 retries. | None (Saga compensation). | Saga emits `InventoryReleased` AND `PaymentRefundRequested`; transitions to `CANCELLED`. | Security analyst reviews flag in `/metrics` / read model; automated refund is immediate. | Dual compensation ensures both reserved inventory and captured payment are safely reverted. |
| **FM-05** | **Shipment Dispatch Failure (Carrier API Outage)** | `ShippingService` | Transient or Permanent | Carrier API down or `POST /chaos/shipping/failure` | `ShippingService` emits `ShipmentFailed`. | Retried 3 times internally; if permanent, emits `ShipmentFailed`. | Logged to DLQ if unhandled exception occurs. | Saga triggers dual rollbacks: `InventoryReleased` and `PaymentRefundRequested`. | Order marked `CANCELLED`; customer notified; operator can check carrier logs. | Full financial and inventory consistency restored; no orphaned stock reservations. |
| **FM-06** | **Kafka Partition Rebalance & Duplicate Event Delivery** | Platform / All Consumers | Infrastructure Event | Broker rebalance, consumer group scale-up, or network partition | `BaseConsumer.handleMessage` queries `processed_events(event_id, consumer_name)`. | N/A (Duplicate skipped immediately). | None. | None required (Domain logic bypassed). | Automatic; consumer commits offset and resumes stream processing. | `UNIQUE(event_id, consumer_name)` guarantees zero duplicate side-effects. |
| **FM-07** | **Corrupted / Malformed Payload (Poison Pill)** | Platform / All Consumers | Data Corruption (Non-Retryable) | Malformed JSON byte string on Kafka topic | `BaseConsumer` schema validation failure (`JSON.parse` / `Zod` validation error). | 0 retries (Poison pills must never block partitions). | Immediate quarantine to `platform.dlq` + `dlq_messages` table with raw payload. | None. | Kafka offset committed immediately to prevent lag backlog; operator inspects payload in `/dlq`. | Poison pill quarantined without halting consumer partition processing. |
| **FM-08** | **Outbox Relay Crash Mid-Batch** | `OutboxRelay` | Node Crash / Process Kill | Host kill (`SIGKILL`) during outbox sweep | Background relay heartbeat / lease timeout. | Automatic recovery on next relay cycle or failover worker. | Events exceeding 10 publication attempts marked `FAILED`. | None. | New outbox worker reclaims leases older than 5 minutes (`created_at < NOW() - INTERVAL '5m'`). | Idempotent consumers downstream deduplicate if an event was published before crash. |
| **FM-09** | **Read Model Materialized View Drift / Corruption** | CQRS Projections | Projection Loss | Manual database truncate or lost projection events | Read model query returns empty or out-of-date state. | N/A (Read model reconstruction via replay). | None. | None. | Operator invokes `POST /replay { "aggregate_id": "ord_123" }` to rebuild projection from `event_store`. | `event_store` is immutable (DB trigger protected); guarantees 100% deterministic reconstruction. |
| **FM-10** | **Concurrent Outbox Polling by Multiple Node Instances** | `OutboxRelay` | Concurrency Hazard | High load with multiple horizontal API/Relay replicas | Handled by SQL query design. | Built-in database locking mechanism. | None. | None. | Automatic; each worker obtains mutually exclusive event IDs via `FOR UPDATE SKIP LOCKED`. | Zero duplicate message generation at outbox polling boundary. |

---

## 3. Detailed Failure Recovery Scenarios

### Scenario A: Cascading Multi-Service Compensation
```mermaid
sequenceDiagram
    autonumber
    participant Order as OrderService
    participant Saga as SagaOrchestrator
    participant Pay as PaymentService
    participant Inv as InventoryService
    participant Fraud as FraudService
    participant Read as CQRS Projections

    Order->>Saga: OrderCreated (Outbox Relay)
    Saga->>Pay: PaymentRequested
    Pay-->>Saga: PaymentAuthorized (Tx: pay_100)
    Saga->>Inv: InventoryReservationRequested
    Inv-->>Saga: InventoryReserved (Res: res_200)
    Saga->>Fraud: FraudCheckRequested
    Fraud-->>Saga: FraudRejected (Risk Score: 95)
    Note over Saga: SAGA COMPENSATING TRIGGERED
    par Compensation Step 1
        Saga->>Inv: InventoryReleased (Res: res_200)
        Inv-->>Read: Stock Restored to available_stock
    and Compensation Step 2
        Saga->>Pay: PaymentRefundRequested (Tx: pay_100)
        Pay-->>Saga: PaymentRefunded (Refund: ref_300)
    end
    Saga->>Order: OrderCancelled
    Order-->>Read: order_read_model status = 'CANCELLED'
```

---

## 4. Operational Runbook for Dead Letter Queue (DLQ) Incidents

### Step 1: Alert & Metric Inspection
When consumer metric `failures > 0` or `/dlq` reports unresolved messages:
```bash
curl -s http://localhost:3000/dlq | jq .
```

### Step 2: Root Cause Diagnosis
Inspect `error_message`, `stack_trace`, and `payload.raw_message` in the DLQ record. Common causes:
1. **Downstream 3rd Party Outage:** Temporary upstream network failure.
2. **Schema Incompatibility:** Producer sent an unsupported envelope version.
3. **Database Constraint Violation:** Unhandled edge case payload.

### Step 3: Resolution & Replay
Once the underlying issue (e.g. gateway reachability or schema patch) is resolved:
```bash
# Replay specific DLQ event back to original target topic
curl -X POST http://localhost:3000/dlq/dlq_99a8b1c2/replay | jq .
```
The replay service will:
1. Delete the prior `FAILED` entry from `processed_events` using `resetProcessedEventForReplay`.
2. Republish the original event envelope to the target topic with `replayed-from-dlq` header.
3. Mark the DLQ database record as `REPLAYED`.

---

## 5. Summary of Automated Verification

Every failure scenario and recovery mechanism in this matrix is covered by automated regression and integration test suites:
* `tests/unit/saga.test.ts` — Happy path, payment declination, inventory shortage, fraud rejection rollbacks.
* `tests/unit/outbox.test.ts` — Atomic database writes, relay publishing, and error retries.
* `tests/unit/idempotency.test.ts` — Deduplication against duplicate message deliveries.
* `tests/unit/red-team.test.ts` — Outbox concurrent claiming, out-of-order saga rejection, DLQ replay idempotency reset, and malformed payload quarantine.
* `tests/e2e/fulfillment-flow.test.ts` — Complete asynchronous fulfillment and compensation lifecycles.
