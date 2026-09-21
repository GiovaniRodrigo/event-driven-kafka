# 11. Event Replay & State Reconstruction Engine

**Version:** 1.1.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED & PRODUCTION HARDENED  

---

## 1. Replay Capabilities

Event Replay allows the platform to re-evaluate historical events from the immutable `event_store` or dead-letter queues.

### Supported Use Cases:
1. **Projection Reconstruction:** Rebuilding `order_read_model` and associated models after a code upgrade or corruption.
2. **Aggregate Reprocessing:** Re-running events for a specific `order_id` in deterministic sequence order (`sequence_number ASC`).
3. **Dead Letter Recovery:** Re-submitting a resolved dead-letter event back to its origin Kafka topic after a downstream bugfix.

```mermaid
sequenceDiagram
    actor Operator
    participant API as Replay API (POST /replay)
    participant Engine as Event Replay Service
    participant Store as PostgreSQL event_store
    participant Proj as ProjectionConsumer

    Operator->>API: POST /replay { aggregate_id: "ord_123" }
    API->>Engine: replayAggregate("ord_123")
    Engine->>Store: resetReadModelForAggregate("ord_123")
    Engine->>Store: SELECT * FROM event_store WHERE aggregate_id = 'ord_123' ORDER BY sequence_number ASC
    Store-->>Engine: [OrderCreated (seq:1), PaymentAuthorized (seq:2), ...]
    loop Chronological Re-projection
        Engine->>Proj: applyHistoricalEvent(envelope, client)
        Proj->>Store: INSERT INTO projection_applied_events ...
        Proj->>Store: UPDATE order_read_model / inventory_read_model
    end
    Engine-->>API: ReplayResult { events_processed: 6, duration_ms: 12, reconstructed_state: {...} }
    API-->>Operator: 200 OK
```

---

## 2. Safety Guards Against Accidental Replay
* **Scoped Target:** Replays must explicitly specify an `aggregate_id` or `dlq_id`.
* **Isolated Projection Handler (`applyHistoricalEvent`):** Historical replays bypass live event store appending and websocket emissions, directly rebuilding read models in isolated transactions.
* **Projection Idempotency (`projection_applied_events`):** Replayed events track their application in `projection_applied_events(projection_name, event_id)` to prevent double-counting incremental mutations (e.g. inventory reservations).
* **Deterministic Sequencing:** Event streams are fetched with `ORDER BY sequence_number ASC, occurred_at ASC`, ensuring repeatable reconstructed states.
* **Audit Logging:** Every replay operation generates a unique `replay_id` and structured audit log entry.
