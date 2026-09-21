# 14. Failure & Recovery Scenarios

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Scenario Catalog

### Scenario 1: Payment Declination
* **Trigger:** Customer credit card fails authorization or `POST /chaos/payment/failure`.
* **Flow:** `OrderCreated` → `PaymentRequested` → `PaymentRejected` → `OrderFailed`.
* **State Machine:** Reaches `FAILED` terminal state. No money is captured; no inventory is reserved.

### Scenario 2: Inventory Shortage & Payment Compensation
* **Trigger:** Order placed for `OUT_OF_STOCK_ITEM` or `POST /chaos/inventory/failure`.
* **Flow:** `OrderCreated` → `PaymentRequested` → `PaymentAuthorized` → `InventoryReservationRequested` → `InventoryReservationFailed` → `PaymentRefundRequested` → `PaymentRefunded` → `OrderCancelled`.
* **Recovery:** Saga enters `COMPENSATING`, refunds the captured payment, and marks the order `CANCELLED`.

### Scenario 3: Fraud Rejection & Multi-Service Compensation
* **Trigger:** High-risk order amount (> $10,000) or `POST /chaos/fraud/rejection`.
* **Flow:** `OrderCreated` → `PaymentAuthorized` → `InventoryReserved` → `FraudCheckRequested` → `FraudRejected`.
* **Compensation:** Saga dispatches both `InventoryReleased` and `PaymentRefundRequested`. Once resolved, the order is safely `CANCELLED`.

### Scenario 4: Duplicate Message Delivery
* **Trigger:** Kafka partition rebalance or network glitch redelivering the same offset.
* **Flow:** Consumer receives identical `event_id` → Checks `processed_events` table → Finds existing record → Logs `duplicate_event_skipped` and skips domain logic without duplicating effects.

### Scenario 5: Poison Pill / Malformed Event Routing to DLQ
* **Trigger:** Corrupted non-JSON string published to Kafka topic.
* **Flow:** Consumer fails envelope parse → Categorizes as Non-Retryable Error → Saves to `dlq_messages` table + publishes to `platform.dlq` topic → Acknowledges offset to prevent consumer lag blocking.

### Scenario 6: Read Model Reconstruction via Event Replay
* **Trigger:** Projection database corruption or table drop.
* **Flow:** Operator calls `POST /replay { "aggregate_id": "ord_123" }` → `EventReplayService` reads historical events from `event_store` → Reconstructs `order_read_model` to exact consistency.
