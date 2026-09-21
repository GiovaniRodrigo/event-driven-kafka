# 07. Kafka Partitioning & Message Ordering

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Partition Key Strategy

In distributed event streaming with Apache Kafka, total global ordering across all partitions does not exist. However, business domain aggregates (e.g. an e-commerce Order) require **strict FIFO sequence per aggregate**:
* `OrderCreated` **MUST** be processed before `PaymentAuthorized`.
* `PaymentAuthorized` **MUST** be processed before `InventoryReserved`.
* `InventoryReserved` **MUST** be processed before `ShipmentCreated`.

### The Partition Key Rule:
Every message produced to any event stream in this platform uses the `aggregate_id` (i.e. `order_id`) as the Kafka message key.

```typescript
const record = {
  key: envelope.aggregate_id, // e.g. "ord_8f1b2c3d"
  value: JSON.stringify(envelope)
};
```

---

## 2. Murmur2 Consistent Hashing

Kafka's default partitioner hashes the message key using the **Murmur2** algorithm:
$$\text{Partition} = \text{toPositive}(\text{Murmur2}(\text{key})) \pmod{\text{Number of Partitions}}$$

Because the hashing function is deterministic, all events for a given `order_id` land on the exact same partition:

```mermaid
flowchart TD
    E1["Event: ord_A (OrderCreated)"] -->|Murmur2('ord_A') % 3| P0["Partition 0"]
    E2["Event: ord_B (OrderCreated)"] -->|Murmur2('ord_B') % 3| P1["Partition 1"]
    E3["Event: ord_A (PaymentAuthorized)"] -->|Murmur2('ord_A') % 3| P0
    E4["Event: ord_A (InventoryReserved)"] -->|Murmur2('ord_A') % 3| P0
    E5["Event: ord_C (OrderCreated)"] -->|Murmur2('ord_C') % 3| P2["Partition 2"]
```

---

## 3. Consumer Group Parallelism

* A partition within a topic can only be assigned to **one single consumer** instance within a Consumer Group.
* With 3 partitions per topic, 3 worker nodes can process events in parallel.
* Events across different orders are processed concurrently; events for the same order are processed sequentially.
