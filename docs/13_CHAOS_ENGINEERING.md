# 13. Chaos Engineering & Fault Simulation Engine

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Principles of Controlled Chaos

A distributed event-driven system cannot be deemed production-ready without verifying its failure and self-healing mechanisms under realistic adverse conditions.

The platform embeds a dedicated, non-intrusive **Chaos Simulation Engine** (`src/chaos/chaos-engine.ts`) that is **DISABLED BY DEFAULT**.

---

## 2. Chaos Controls REST API

| Endpoint | Method | Purpose | Simulated Failure Mode |
| :--- | :---: | :--- | :--- |
| `/chaos/status` | `GET` | View active fault injection status | Inspect active chaos rules |
| `/chaos/payment/failure` | `POST` | Force payment gateway credit decline | Triggers immediate `PaymentRejected` & `OrderFailed` |
| `/chaos/inventory/failure` | `POST` | Force inventory out-of-stock | Triggers `InventoryReservationFailed` & Saga Compensation (`PaymentRefunded` → `OrderCancelled`) |
| `/chaos/fraud/rejection` | `POST` | Force excessive fraud risk score | Triggers `FraudRejected` & Multi-step Compensation (`InventoryReleased` + `PaymentRefunded`) |
| `/chaos/fraud/latency` | `POST` | Inject network delay in fraud service | Simulates downstream API lag & timeout resilience |
| `/chaos/shipping/failure` | `POST` | Force carrier dispatch failure | Triggers `ShipmentFailed` & Saga Compensation |
| `/chaos/notification/down` | `POST` | Simulate mail server crash | Triggers consumer retry backoff loop and DLQ routing |
| `/chaos/reset` | `POST` | Restore normal operations | Disables all chaos faults |
