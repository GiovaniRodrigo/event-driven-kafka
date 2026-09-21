# 18. Production Hardening & Resource Protection

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Resource Limits & Calibration

To prevent OOM-kills or host freezes on developer machines and production nodes:
* **Docker Ceilings:** Every container has explicit `mem_limit` and `cpus` allocated in `docker-compose.yml`.
* **JVM Heap Caps:** ZooKeeper and Kafka heaps are capped via `-Xmx` / `-Xms` calculated dynamically by `scripts/start.sh`.
* **Node.js RSS Cap:** Configured via `--max-old-space-size`.
* **Host Watchdog:** Active background process (`scripts/watchdog.sh`) monitoring `/proc/meminfo` and gracefully stopping containers if host RAM dips below emergency thresholds.

---

## 2. Graceful Shutdown

Handling `SIGTERM` and `SIGINT`:
1. Closes HTTP listener to reject new incoming requests.
2. Stops `OutboxRelay` loop.
3. Disconnects Kafka consumers after processing the current message payload.
4. Closes Socket.IO connections.
5. Drains and disconnects PostgreSQL connection pool.
6. Exits cleanly with status code 0.
