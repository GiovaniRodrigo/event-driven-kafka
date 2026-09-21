# 03. Event Contracts & Envelope Specification

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Universal Event Envelope

Every event published to Apache Kafka conforms to the following standardized JSON envelope, validated at the boundary via Zod schemas (`src/contracts/envelope.ts`).

```json
{
  "event_id": "evt_d3b07384-d113-469b-9c69-238bd7584310",
  "event_type": "OrderCreated",
  "event_version": 1,
  "aggregate_id": "ord_8f1b2c3d",
  "aggregate_type": "Order",
  "occurred_at": "2026-09-21T02:00:00.000Z",
  "producer": "order-service",
  "correlation_id": "corr_8f1b2c3d-99a1",
  "causation_id": "req_8f1b2c3d",
  "schema_version": 1,
  "payload": {
    "order_id": "ord_8f1b2c3d",
    "user_id": "usr_48102",
    "items": [
      {
        "sku": "LAPTOP-001",
        "name": "High-Performance Workstation",
        "price": 1999.99,
        "quantity": 1
      }
    ],
    "total_amount": 1999.99,
    "currency": "USD",
    "shipping_address": {
      "street": "123 Tech Lane",
      "city": "Austin",
      "zip": "78701",
      "country": "US"
    }
  }
}
```

### Envelope Field Definitions

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `event_id` | `string` (UUID) | Globally unique identifier for this specific event instance. |
| `event_type` | `string` | PascalCase identifier matching domain event type (e.g. `OrderCreated`). |
| `event_version` | `number` | Version of the event structure (starts at 1). |
| `aggregate_id` | `string` | Primary identifier of the domain aggregate (e.g. `ord_8f1b2c3d`). Used as the Kafka message key. |
| `aggregate_type` | `string` | Bounded context entity type (`Order`, `Payment`, `Inventory`, `Fraud`, `Shipment`, `Notification`). |
| `occurred_at` | `string` (ISO8601) | Exact UTC timestamp when the business event took place. |
| `producer` | `string` | Microservice or component that created and emitted the event. |
| `correlation_id` | `string` | End-to-end trace ID linking all events in the same business lifecycle. |
| `causation_id` | `string` | Direct parent `event_id` or command ID that directly caused this event. |
| `schema_version` | `number` | Version of the Zod schema payload validator. |
| `payload` | `Record<string, unknown>` | Strongly-typed domain payload validated against domain Zod schemas. |

---

## 2. Event Types & Schemas

### 2.1 Order Context
* **`OrderCreated`**: Emitted when a new order command is accepted and written to the transactional outbox.
* **`OrderCancelled`**: Emitted when a saga fails and all compensations are completed.
* **`OrderCompleted`**: Terminal event indicating successful fulfillment of Payment, Inventory, Fraud, and Shipping.
* **`OrderFailed`**: Terminal event indicating non-compensable or permanent business failure.

### 2.2 Payment Context
* **`PaymentRequested`**: Command event dispatched by Saga Orchestrator to authorize funds.
* **`PaymentAuthorized`**: Emitted upon successful credit authorization.
* **`PaymentRejected`**: Emitted when card/funds are declined or payment gateway times out.
* **`PaymentRefundRequested`**: Compensating command event dispatched when downstream steps fail.
* **`PaymentRefunded`**: Emitted when funds are returned to the customer.

### 2.3 Inventory Context
* **`InventoryReservationRequested`**: Command event dispatched by Saga to lock items in stock.
* **`InventoryReserved`**: Emitted when all order items have sufficient stock reserved.
* **`InventoryReservationFailed`**: Emitted when any SKU is out of stock.
* **`InventoryReleased`**: Compensating event emitted when previously locked stock is unlocked.

### 2.4 Fraud Context
* **`FraudCheckRequested`**: Command event to trigger automated risk scoring.
* **`FraudApproved`**: Emitted when risk score is within acceptable bounds (< 75).
* **`FraudRejected`**: Emitted when risk score exceeds threshold (>= 75).

### 2.5 Shipping Context
* **`ShipmentRequested`**: Command event to create shipment manifest and tracking number.
* **`ShipmentCreated`**: Emitted when carrier confirms package pickup/label creation.
* **`ShipmentFailed`**: Emitted if shipping address is invalid or carrier rejects shipment.

### 2.6 Notification Context
* **`NotificationRequested`**: Command event dispatched on order status milestones.
* **`NotificationSent`**: Emitted when email/SMS/push delivery is confirmed.
