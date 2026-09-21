# 20. Distributed Failure & Resilience Matrix

**Project:** Event-Driven Kafka Fulfillment Platform  
**Version:** 2.1.0  
**Author:** Giovani Rodrigo ([giovanif245@gmail.com](mailto:giovanif245@gmail.com))  
**Status:** DISTRIBUTED VERIFICATION GATED (STRICT INFRASTRUCTURE INTEGRITY)  

---

## 1. Executive Resilience Architecture Overview

In an asynchronous distributed event-driven system operating over Apache Kafka and PostgreSQL, failures across networks, brokers, workers, and database connections are expected conditions. The platform enforces resilience governed by five foundational pillars:

1. **At-Least-Once Delivery with Consumer-Scoped Idempotency:** Guarantees zero lost messages across network partitions while eliminating duplicate side effects within each consumer group via `processed_events(event_id, consumer_name)` (`UNIQUE` composite constraint).
2. **Transactional Outbox with Lease Fencing:** Eliminates dual-write anomalies using PostgreSQL `BEGIN ... COMMIT` transactions, concurrent row leasing via `FOR UPDATE SKIP LOCKED`, and conditional status updates (`status = 'PROCESSING' AND lease_owner = $worker`) that reject stale workers with `rowCount === 0`.
3. **Durable DLQ Crash Consistency (`dlq_outbox`):** Transactionally co-locates dead-letter recording, failed idempotency markers, and dead-letter outbox emissions within a single database transaction, preventing DLQ event loss across process crashes.
4. **Saga Compensation Barriers with Row Locking:** Guarantees atomic state transitions and monotonic convergence during concurrent multi-step distributed rollbacks using `SELECT ... FOR UPDATE` row locking.
5. **Deterministic Event Replay with Pure Projection Isolation:** Enables read-model rebuilding from immutable `event_store` records ordered by explicit `sequence_number ASC` via `applyHistoricalEvent`, with zero external egress, zero WebSocket broadcasts, and zero Kafka emissions.

---

## 2. Comprehensive Distributed Failure Matrix

| ID | Failure Scenario | Detection Mechanism | Protection & Invariant | Recovery Protocol | Verification Test Suite | Test Type | Infrastructure Required | Verification Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :---: |
| **FM-01** | **Payment Decline (Invalid Card / Expired)** | `PaymentService` domain evaluation | Immediate `PaymentRejected` event emission | Saga transitions to `FAILED`; order marked `FAILED` | `tests/unit/saga.test.ts` | Unit | None (Mocked) | **VERIFIED** |
| **FM-02** | **Payment Gateway Network Timeout** | `BaseConsumer` execution timeout caught | Exponential backoff (`500ms * 2^attempt + jitter`, max 3) | If retries exhausted, routed to `dlq_messages` + `dlq_outbox` | `tests/unit/dlq-crash-consistency.test.ts`, `tests/integration/dlq-recovery.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-03** | **Inventory Shortage (Out of Stock)** | `InventoryService` stock availability check | Emits `InventoryReservationFailed`; triggers compensation | Saga enters `COMPENSATING`; emits `PaymentRefundRequested` -> `OrderCancelled` | `tests/unit/saga.test.ts`, `tests/e2e/fulfillment-flow.test.ts` | Unit / E2E | None (Simulator) | **VERIFIED** |
| **FM-04** | **High-Risk Fraud Rejection (Score > 80)** | `FraudService` rule evaluation | Emits `FraudRejected`; triggers dual compensation | Dual barrier tracks `INVENTORY_RELEASE` + `PAYMENT_REFUND` before `OrderCancelled` | `tests/unit/saga-compensation-barrier.test.ts` | Unit | None (Mocked) | **VERIFIED** |
| **FM-05** | **Shipment Dispatch Failure** | `ShippingService` carrier error catch | Emits `ShipmentFailed`; triggers rollback | Saga triggers inventory release and payment refund | `tests/unit/saga.test.ts` | Unit | None (Mocked) | **VERIFIED** |
| **FM-06** | **Kafka Partition Rebalance & Redelivery** | `BaseConsumer` checks `processed_events` | `UNIQUE(event_id, consumer_name)` skips duplicate | Consumer commits offset immediately; domain logic bypassed | `tests/unit/idempotency.test.ts` | Unit | None (Mocked) | **VERIFIED** |
| **FM-07** | **Corrupted / Malformed Payload (Poison Pill)** | `BaseConsumer` Zod / JSON parse catch | Atomic quarantine to `dlq_messages` + `dlq_outbox` | Consumer commits offset, preventing partition stall | `tests/unit/dlq-crash-consistency.test.ts`, `tests/unit/red-team.test.ts` | Unit | None (Mocked) | **VERIFIED** |
| **FM-08** | **Stale Outbox Worker Lease Expiration** | Conditional SQL update check (`rowCount === 0`) | Stale worker rejected (`STALE_WORKER_LOST_LEASE`) | Active worker owns lease and publishes to Kafka | `tests/unit/outbox-lease-fencing.test.ts`, `tests/integration/outbox-concurrency.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-09** | **Read Model Corruption / View Drift** | Read model query returns inconsistent state | Replay service rebuilds projection via `event_store` | Operator triggers `POST /replay { aggregate_id }` | `tests/unit/replay-determinism.test.ts`, `tests/integration/replay-rebuild.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-10** | **Concurrent Outbox Polling by Workers** | SQL `FOR UPDATE SKIP LOCKED` | Each worker receives disjoint set of outbox rows | Parallel non-blocking message publication | `tests/unit/red-team.test.ts`, `tests/integration/outbox-concurrency.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-11** | **Concurrent Saga Compensation Arrival** | Row lock via `SELECT ... FOR UPDATE` | Monotonic accumulation of `compensations_completed` | Exactly one worker satisfies barrier and emits `OrderCancelled` | `tests/unit/failure-injection.test.ts`, `tests/integration/saga-compensation-concurrency.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-12** | **Duplicate Events During Projection Replay** | `projection_applied_events` tracking | Duplicate historical mutations skipped | Read models reconstructed deterministically | `tests/unit/replay-determinism.test.ts`, `tests/integration/replay-rebuild.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-13** | **Kafka Broker Outage During Outbox Publishing** | Outbox publish catches network error | Row returned to `PENDING`; `attempts` incremented | Outbox relay retries on next poll tick after broker recovers | `tests/unit/outbox-crash.test.ts` | Unit | None (Mocked) | **VERIFIED** |
| **FM-14** | **Aggregate Event Stream Out-of-Order Append** | `pg_advisory_xact_lock` + `UNIQUE(aggregate_id, seq)` | Concurrent appends serialized; duplicate sequence aborted | Writers append strict monotonic sequence (1..N) | `tests/unit/event-store-rebuild.test.ts`, `tests/integration/event-store-sequence.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-15** | **DLQ Replay Blocked by Previous Failed Marker** | `resetProcessedEventForReplay` invoked | Previous `FAILED` marker in `processed_events` deleted | Consumer safely re-executes replayed event | `tests/unit/dlq-crash-consistency.test.ts`, `tests/integration/dlq-recovery.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-16** | **Process Crash Between DLQ Write and Publish** | Database transaction atomicity | `dlq_outbox` committed in same TX as `dlq_messages` | Background relay publishes pending `dlq_outbox` after restart | `tests/unit/dlq-crash-consistency.test.ts`, `tests/integration/dlq-recovery.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-17** | **Out-of-Order Saga Step Events (Late Arrival)** | Explicit state transition guards in orchestrator | Event dropped if saga is not in expected prerequisite state | Orchestrator logs warning without corrupting saga state | `tests/unit/red-team.test.ts` | Unit | None (Mocked) | **VERIFIED** |
| **FM-18** | **Event Store Historical Mutation Attempt** | PostgreSQL trigger `trg_prevent_event_store_mutation` | Aborts SQL UPDATE/DELETE with fatal exception | Append-only immutability enforced at database engine level | `tests/unit/event-store-rebuild.test.ts`, `tests/integration/event-store-sequence.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-19** | **Side-Effect Leakage During Historical Replay** | Method separation: `applyHistoricalEvent` vs `emitLiveNotifications` | Replay engine invokes DB mutation only; zero WebSocket calls | Zero duplicate client notifications or external side effects | `tests/unit/failure-injection.test.ts`, `tests/integration/replay-rebuild.integration.test.ts` | Unit / Integration | PostgreSQL | **VERIFIED** |
| **FM-20** | **Kafka Offset Commit & Consumer Restart Recovery** | Kafka consumer group offset tracking | Committed offsets not re-processed upon consumer restart | Consumer resumes from latest committed offset | `tests/integration/kafka-messaging.integration.test.ts` | Integration | Kafka | **VERIFIED** |

---

## 3. Strict Verification Classification Legend

To prevent misleading claims of verification:
- **`VERIFIED` (Unit)**: Verified in automated unit test suites with simulated concurrency, mocks, and memory stores.
- **`VERIFIED` (Integration / CI)**: Verified against real PostgreSQL and Apache Kafka broker services in GitHub Actions CI and local integration harnesses.
- **`NOT VERIFIED`**: Scenarios that have not been executed against real infrastructure and lack automated test proof.
