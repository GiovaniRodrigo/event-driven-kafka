# 01. System Architecture & Event-Driven Topology

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Architectural Philosophy

This platform implements an end-to-end **Event-Driven Architecture (EDA)** for an e-commerce order fulfillment lifecycle. It enforces strict distributed systems patterns to eliminate data inconsistency, phantom messages, duplicate processing, and unhandled failure states.

### Core Principles:
1. **Asynchronous Autonomy:** Commands are accepted via HTTP `202 Accepted` and immediately handed off to the event stream. Bounded contexts communicate purely via asynchronous Kafka events.
2. **Atomic State & Publication (Transactional Outbox):** No dual-write vulnerabilities. Aggregate changes and their resulting domain events are persisted within a single local database transaction.
3. **Idempotent Consumers:** Every consumer safely survives message duplication, replay, rebalances, and network retries using isolated, persistent deduplication guards.
4. **Coordinated Consistency (Saga Pattern):** A persistent Saga Orchestrator manages the multi-step fulfillment workflow across Payment, Inventory, Fraud, and Shipping, executing automated compensating transactions on failure.
5. **Command-Query Responsibility Segregation (CQRS):** Write operations append events and update outbox records; read queries query dedicated, re-projectable read models.
6. **Observability & Traceability:** Every event carries a universal envelope with `correlation_id`, `causation_id`, `event_id`, and `occurred_at`, enabling end-to-end distributed tracing.

---

## 2. High-Level System Architecture Diagram

```mermaid
flowchart TD
    Client["Client / Load Generator / k6"]
    
    subgraph REST_API["HTTP & Realtime Transport"]
        OrderRoutes["POST /orders\nGET /orders/:id\nGET /metrics\nGET /health\nGET /ready\nPOST /replay\nGET /dlq"]
        SocketServer["Socket.IO Real-time Gateway"]
    end

    subgraph OrderContext["Order Bounded Context (Command Side)"]
        OrderService["Order Service"]
        DB_Tx[("Postgres Transaction\n- orders\n- outbox_events")]
        OutboxRelay["Transactional Outbox Relay"]
    end

    subgraph KafkaCluster["Apache Kafka Cluster"]
        T_Orders["orders.events"]
        T_Payments["payments.events"]
        T_Inventory["inventory.events"]
        T_Fraud["fraud.events"]
        T_Shipping["shipping.events"]
        T_Notifications["notifications.events"]
        T_Retry["*.retry"]
        T_DLQ["*.dlq"]
    end

    subgraph SagaContext["Saga Orchestration Context"]
        SagaOrchestrator["Order Fulfillment Saga Orchestrator"]
        SagaState[("saga_instances")]
    end

    subgraph DomainHandlers["Domain Handlers / Consumers"]
        PaymentConsumer["Payment Consumer\n(payment-group)"]
        InventoryConsumer["Inventory Consumer\n(inventory-group)"]
        FraudConsumer["Fraud Consumer\n(fraud-group)"]
        ShippingConsumer["Shipping Consumer\n(shipping-group)"]
        NotificationConsumer["Notification Consumer\n(notification-group)"]
    end

    subgraph ProjectionContext["CQRS & Query Side"]
        ProjectionConsumer["Projection Consumer\n(projection-group)"]
        EventStore[("event_store")]
        ReadModels[("Read Models\n- order_read_model\n- payment_read_model\n- inventory_read_model\n- shipment_read_model\n- dashboard_metrics")]
    end

    subgraph OpsControls["Operations, Resilience & Chaos"]
        ChaosEngine["Chaos Injection Controls"]
        DLQManager["DLQ Inspector & Replay"]
    end

    Client -->|HTTP POST /orders| OrderRoutes
    OrderRoutes --> OrderService
    OrderService --> DB_Tx
    OutboxRelay -->|Polls outbox_events| DB_Tx
    OutboxRelay -->|Publishes OrderCreated| T_Orders

    T_Orders --> SagaOrchestrator
    SagaOrchestrator -->|PaymentRequested| T_Payments
    T_Payments --> PaymentConsumer
    PaymentConsumer -->|PaymentAuthorized / Rejected| T_Payments

    T_Payments --> SagaOrchestrator
    SagaOrchestrator -->|InventoryReservationRequested| T_Inventory
    T_Inventory --> InventoryConsumer
    InventoryConsumer -->|InventoryReserved / Failed| T_Inventory

    T_Inventory --> SagaOrchestrator
    SagaOrchestrator -->|FraudCheckRequested| T_Fraud
    T_Fraud --> FraudConsumer
    FraudConsumer -->|FraudApproved / Rejected| T_Fraud

    T_Fraud --> SagaOrchestrator
    SagaOrchestrator -->|ShipmentRequested| T_Shipping
    T_Shipping --> ShippingConsumer
    ShippingConsumer -->|ShipmentCreated / Failed| T_Shipping

    T_Shipping --> SagaOrchestrator
    SagaOrchestrator -->|NotificationRequested| T_Notifications
    T_Notifications --> NotificationConsumer
    NotificationConsumer -->|NotificationSent| T_Notifications

    KafkaCluster --> ProjectionConsumer
    ProjectionConsumer --> EventStore
    ProjectionConsumer --> ReadModels
    ProjectionConsumer --> SocketServer
    ReadModels --> OrderRoutes
    SocketServer -.->|Websocket push| Client
```

---

## 3. Order Fulfillment State Machine & Event Flow

### 3.1 Happy Path (Successful Fulfillment)
```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as HTTP API
    participant Outbox as Outbox Relay
    participant Kafka as Kafka Broker
    participant Saga as Saga Orchestrator
    participant Payment as Payment Service
    participant Inventory as Inventory Service
    participant Fraud as Fraud Service
    participant Shipping as Shipping Service
    participant Notification as Notification Service
    participant Proj as Projection / Read Model

    Client->>API: POST /orders
    API-->>Client: 202 Accepted (order_id, correlation_id)
    Outbox->>Kafka: Publish OrderCreated [orders.events]
    Kafka->>Saga: OrderCreated
    Saga->>Kafka: Publish PaymentRequested [payments.events]
    Kafka->>Payment: PaymentRequested
    Payment->>Kafka: Publish PaymentAuthorized [payments.events]
    Kafka->>Saga: PaymentAuthorized
    Saga->>Kafka: Publish InventoryReservationRequested [inventory.events]
    Kafka->>Inventory: InventoryReservationRequested
    Inventory->>Kafka: Publish InventoryReserved [inventory.events]
    Kafka->>Saga: InventoryReserved
    Saga->>Kafka: Publish FraudCheckRequested [fraud.events]
    Kafka->>Fraud: FraudCheckRequested
    Fraud->>Kafka: Publish FraudApproved [fraud.events]
    Kafka->>Saga: FraudApproved
    Saga->>Kafka: Publish ShipmentRequested [shipping.events]
    Kafka->>Shipping: ShipmentRequested
    Shipping->>Kafka: Publish ShipmentCreated [shipping.events]
    Kafka->>Saga: ShipmentCreated
    Saga->>Kafka: Publish OrderCompleted [orders.events]
    Saga->>Kafka: Publish NotificationRequested [notifications.events]
    Kafka->>Notification: NotificationRequested
    Notification->>Kafka: Publish NotificationSent [notifications.events]
    Kafka->>Proj: All Events Processed -> Read Models Updated
```

### 3.2 Failure & Compensating Transactions
```mermaid
sequenceDiagram
    autonumber
    participant Kafka as Kafka Broker
    participant Saga as Saga Orchestrator
    participant Inventory as Inventory Service
    participant Payment as Payment Service
    participant Proj as Projection / Read Model

    Note over Saga: State: PAYMENT_APPROVED
    Saga->>Kafka: Publish InventoryReservationRequested [inventory.events]
    Kafka->>Inventory: InventoryReservationRequested
    Inventory->>Kafka: Publish InventoryReservationFailed [inventory.events]
    Kafka->>Saga: InventoryReservationFailed (Trigger Compensation)
    Note over Saga: State: COMPENSATING
    Saga->>Kafka: Publish PaymentRefundRequested [payments.events]
    Kafka->>Payment: PaymentRefundRequested
    Payment->>Kafka: Publish PaymentRefunded [payments.events]
    Kafka->>Saga: PaymentRefunded
    Saga->>Kafka: Publish OrderCancelled [orders.events]
    Note over Saga: State: CANCELLED
    Kafka->>Proj: OrderCancelled -> Read Model status = cancelled
```

---

## 4. Bounded Context Boundaries

1. **Order Context (`src/domain/order`):**
   * Manages order lifecycle initiation, validates initial payload, records state to DB, writes to `outbox_events`.
2. **Payment Context (`src/domain/payment`):**
   * Authorizes charges, performs payment captures, executes refunds for cancelled orders.
3. **Inventory Context (`src/domain/inventory`):**
   * Manages SKU stock levels, processes atomic stock reservations, releases reserved stock on compensation.
4. **Fraud Context (`src/domain/fraud`):**
   * Analyzes order metadata and risk factors, issues approvals or fraud rejections.
5. **Shipping Context (`src/domain/shipping`):**
   * Allocates tracking numbers, connects to carriers, dispatches deliveries.
6. **Notification Context (`src/domain/notification`):**
   * Formats and dispatches transactional communications (Email, SMS, Push).
7. **Saga Orchestrator (`src/saga`):**
   * Central coordinator of the distributed transaction, maintains persistent state in `saga_instances`.
8. **Projection Context (`src/application/projections`):**
   * Listens to all domain topics, maintains immutable `event_store` history, builds materialized views.
9. **Platform & Operations (`src/observability`, `src/chaos`, `src/dlq`):**
   * Health checks, readiness probes, dead-letter inspection, fault injection, event replay.
