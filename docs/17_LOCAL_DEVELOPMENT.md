# 17. Local Development Guide

**Version:** 1.0.0  
**Author:** Giovani Rodrigo  
**Status:** IMPLEMENTED  

---

## 1. Quick Start

### 1.1 Prerequisites
* Node.js >= 20.0.0
* Docker & Docker Compose
* PostgreSQL 15 (if running natively outside Docker)

### 1.2 Start the Stack (Calibrated with Host Watchdog)
```bash
# Calibrate memory ceilings and start stack with host-level protection
./scripts/start.sh
```

### 1.3 Native Development
```bash
# Install dependencies
npm install

# Compile TypeScript
npm run build

# Run in development mode
npm run dev

# Run unit tests
npm test
```
