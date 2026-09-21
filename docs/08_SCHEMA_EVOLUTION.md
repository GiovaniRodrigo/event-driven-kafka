# 08. Schema Evolution & Versioning Strategy

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Versioning Architecture

Event-driven microservices must evolve independently without coordinated lockstep deployments. This platform enforces **Backward & Forward Compatibility** using explicit schema versions embedded directly into the event envelope:

```json
{
  "event_type": "OrderCreated",
  "event_version": 1,
  "schema_version": 1,
  "payload": { ... }
}
```

---

## 2. Evolution Rules

1. **Additive Changes (Non-breaking):**
   * Adding new optional fields with default fallback values (e.g. `currency: "USD"` or `shipping_address`).
   * Consumers validate incoming payloads using Zod schemas with default values.
2. **Schema Registry Preparedness:**
   * While this local laboratory runs without an external Confluent Schema Registry container to maintain strict resource budgets (~8GB laptops), all events are defined using strict Zod types (`src/contracts/`), making the codebase 100% prepared for an Avro/Protobuf Schema Registry plug-in.
