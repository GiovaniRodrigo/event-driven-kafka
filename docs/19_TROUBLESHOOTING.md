# 19. Operational Troubleshooting Guide

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Common Scenarios & Diagnostics

### 1.1 Messages Stuck or Sagas not Progressing
* **Check Kafka Consumer Groups:** `GET /consumers` or `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups`
* **Check Outbox Table:** `SELECT status, count(*) FROM outbox_events GROUP BY status;`
* **Check Dead Letter Queue:** `GET /dlq`

### 1.2 Out of Memory / Container Terminated
* **Check Watchdog Logs:** `cat scripts/watchdog.log`
* **Re-run Calibrated Startup:** `./scripts/start.sh`
