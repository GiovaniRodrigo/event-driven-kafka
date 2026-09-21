# 15. Testing Architecture & Test Pyramid

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Test Pyramid Strategy

The testing architecture ensures that every architectural layer is rigorously validated:

```
        / \
       /   \      E2E Tests (Full Outbox -> Kafka -> Saga -> DB -> Socket.IO)
      / ----\
     /       \    Integration Tests (Real PostgreSQL + In-memory Kafka mocks / DB Transactions)
    / --------\
   /           \  Unit Tests (Event Validation, Saga State Machine, Idempotency, Retry, Outbox)
  /_____________\
```

---

## 2. Test Execution Commands

* **Run all tests:** `npm test`
* **Unit tests:** `npm run test:unit`
* **Integration tests:** `npm run test:integration`
* **E2E tests:** `npm run test:e2e`
* **Test Coverage:** `npm test -- --coverage`
