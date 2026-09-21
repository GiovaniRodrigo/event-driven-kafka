# 12. Observability, Distributed Tracing & Health Monitoring

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Structured Logging & Distributed Tracing

Unstructured logs (`console.log("hello")`) are strictly forbidden. All log output is formatted as single-line machine-readable JSON containing distributed trace context:

```json
{
  "timestamp": "2026-09-21T03:00:00.000Z",
  "level": "INFO",
  "event": "message_processed_successfully",
  "consumer": "payment-service",
  "event_id": "evt_8f1b2c3d",
  "event_type": "PaymentRequested",
  "aggregate_id": "ord_8f1b2c3d",
  "correlation_id": "corr_8f1b2c3d",
  "causation_id": "cmd_8f1b2c3d",
  "topic": "payments.events",
  "partition": 1,
  "offset": "42",
  "duration_ms": 14
}
```

---

## 2. Correlation ID vs. Causation ID

* **`correlation_id`**: Originates at the client entry point (`POST /orders`) and propagates unchanged across all downstream hops (Payment, Inventory, Fraud, Shipping, Notifications) to trace the end-to-end lifecycle.
* **`causation_id`**: Identifies the immediate parent event or command that directly triggered this specific step, forming a Directed Acyclic Graph (DAG) of cause and effect.

---

## 3. Real-Time Socket.IO Dashboard

The platform streams real-time metrics and order timeline transitions directly to operational dashboards:
* **Global Channel (`order:created`, `order:updated`)**: Emitted to all connected monitoring clients.
* **Per-Order Room (`order:<id>`)**: Scoped timeline events streamed only to clients inspecting a specific order.
* **Operational Metrics (`metrics:update`)**: Live stream of throughput, active sagas, and DLQ counts every 5 seconds.
