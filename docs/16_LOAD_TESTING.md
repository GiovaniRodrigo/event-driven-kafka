# 16. Load Testing & Performance Benchmarking

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Load Testing Overview

The platform includes automated load test suites using [k6](https://k6.io/) located in `tests/load/`.

### Test Scenarios:
1. **Light Load (100 Orders):** Quick smoke test and baseline latency verification.
2. **Standard Load (1,000 Orders):** Evaluates outbox throughput, consumer concurrency across 3 partitions, and DB connection pool stability.
3. **Stress Load (10,000 Orders):** Tests backpressure limits, batching thresholds, and memory consumption.

---

## 2. Executing Load Tests

```bash
# Run local k6 test script
npm run load-test
```
