# DOCKER + DOCUMENTAÇÃO - Copie e Cole

> ⚠️ **Documento histórico — superado por `src/`.** O código deste guia foi **materializado e corrigido** em `src/` (rodável via `docker compose up --build`; ver [`docs/SETUP.md`](SETUP.md) e [`docs/ARCHITECTURE.md`](ARCHITECTURE.md)). Estes snippets copy-paste são mantidos como referência histórica do plano e **podem conter bugs já corrigidos** no código real — use `src/` como fonte da verdade.

## ARQUIVO 1: docker/Dockerfile

```dockerfile
FROM node:18-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci --only=production

COPY dist/ ./dist/
COPY src/ ./src/

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health', (r) => {if (r.statusCode !== 200) throw new Error(r.statusCode)})"

EXPOSE 3000

CMD ["node", "dist/index.js"]
```

---

## ARQUIVO 2: docker-compose.yml

> **Tetos de recurso (não trave a máquina):** cada serviço tem `mem_limit`,
> `cpus` e teto de heap, injetados via `${...}` a partir de um `.env` que o
> `scripts/start.sh` gera medindo o host. Os fallbacks `:-` deixam
> `docker compose up` rodar sem o script (defaults conservadores pra ~8 GB).
> Suba com `scripts/start.sh` (calibra + sobe + liga o watchdog). Detalhes,
> fórmula e o watchdog: **[07_WATCHDOG.md](07_WATCHDOG.md)**.

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
      KAFKA_HEAP_OPTS: ${ZK_HEAP_OPTS:--Xmx256m -Xms128m}
    mem_limit: ${ZK_MEM_LIMIT:-512m}
    cpus: ${ZK_CPUS:-0.5}
    ports:
      - "2181:2181"
    healthcheck:
      test: echo srvr | nc localhost 2181
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_RETENTION_BYTES: 1073741824
      KAFKA_COMPRESSION_TYPE: snappy
      KAFKA_HEAP_OPTS: ${KAFKA_HEAP_OPTS:--Xmx512m -Xms256m}
    mem_limit: ${KAFKA_MEM_LIMIT:-1250m}
    cpus: ${KAFKA_CPUS:-1.5}
    healthcheck:
      test: kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1
      interval: 10s
      timeout: 5s
      retries: 5

  postgres:
    image: postgres:15-alpine
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: event_driven
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_INITDB_ARGS: "-c shared_buffers=${POSTGRES_SHARED_BUFFERS:-256MB} -c max_connections=200"
    mem_limit: ${POSTGRES_MEM_LIMIT:-512m}
    cpus: ${POSTGRES_CPUS:-1.0}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init-db.sql:/docker-entrypoint-initdb.d/01-init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  app:
    build:
      context: .
      dockerfile: docker/Dockerfile
    container_name: event-driven-api
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    ports:
      - "3000:3000"
    environment:
      NODE_ENV: development
      KAFKA_BROKER: kafka:29092
      DATABASE_URL: postgresql://postgres:postgres@postgres:5432/event_driven
      LOG_LEVEL: info
      NODE_OPTIONS: ${APP_NODE_OPTIONS:---max-old-space-size=384}
    mem_limit: ${APP_MEM_LIMIT:-512m}
    cpus: ${APP_CPUS:-1.0}
    volumes:
      - .:/app
      - /app/node_modules
    command: npm run dev
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:3000/health"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
    driver: local

networks:
  default:
    name: event-driven-network
```

---

## ARQUIVO 3: scripts/init-db.sql

```sql
CREATE TABLE IF NOT EXISTS kafka_topics (
  name VARCHAR(100) PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO kafka_topics (name) VALUES 
  ('orders'),
  ('payments'),
  ('inventory'),
  ('notifications'),
  ('dlq')
ON CONFLICT DO NOTHING;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
```

---

## ARQUIVO 4: docs/SETUP.md

```markdown
# Getting Started

## Prerequisites

- Node.js 18+
- Docker & Docker Compose
- Git

## Local Setup (5 minutes)

### 1. Clone Repository
\`\`\`bash
git clone https://github.com/GiovaniRodrigo/event-driven-kafka
cd event-driven-kafka-architecture
\`\`\`

### 2. Install Dependencies
\`\`\`bash
npm install
\`\`\`

### 3. Start Infrastructure
\`\`\`bash
docker-compose up -d
\`\`\`

Wait 30 seconds for services to become healthy:
\`\`\`bash
docker-compose ps
# All services should show "healthy"
\`\`\`

### 4. Run Application
\`\`\`bash
npm run dev
\`\`\`

Application starts at \`http://localhost:3000\`

## Test It Out

### Create an Order
\`\`\`bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_123",
    "items": [
      {
        "sku": "PROD_001",
        "name": "Sample Product",
        "price": 99.99,
        "quantity": 2
      }
    ]
  }'
\`\`\`

Response:
\`\`\`json
{
  "order_id": "ord_abc123",
  "status": "pending",
  "message": "Order accepted. Processing asynchronously.",
  "created_at": "2026-09-12T10:00:00Z"
}
\`\`\`

### Check Order Status
\`\`\`bash
curl http://localhost:3000/orders/ord_abc123
\`\`\`

### Get Metrics
\`\`\`bash
curl http://localhost:3000/metrics
\`\`\`

## Common Commands

\`\`\`bash
# Development
npm run dev

# Build
npm run build

# Run tests
npm run test:all

# Docker
npm run docker:up
npm run docker:down
npm run docker:logs

# Database
npm run db:migrate
\`\`\`

## Logs

View application logs:
\`\`\`bash
docker-compose logs -f app
\`\`\`

View Kafka logs:
\`\`\`bash
docker-compose logs -f kafka
\`\`\`

View PostgreSQL logs:
\`\`\`bash
docker-compose logs -f postgres
\`\`\`

## Stopping

\`\`\`bash
docker-compose down

# Remove volumes (delete data)
docker-compose down -v
\`\`\`
```

---

## ARQUIVO 5: docs/ARCHITECTURE.md

```markdown
# Event-Driven Architecture

## Overview

This system demonstrates a production-grade event-driven microservices architecture using:
- **Kafka** for event streaming
- **Node.js + TypeScript** for services
- **PostgreSQL** for persistent storage

## Design Principles

### 1. Asynchronous Processing
- Producers emit events without waiting for processing
- Consumers process events independently
- Returns 202 Accepted (not 200 OK)

### 2. Decoupling
- Services don't depend on each other
- Failure in one service doesn't cascade
- Each service owns its data

### 3. Scalability
- Each service scales independently
- Kafka partitions distribute load
- Consumer groups handle parallelism

### 4. Idempotency
- All operations are idempotent
- Duplicate events produce same result
- No side effects from replays

## Data Flow

\`\`\`
1. User creates order (POST /orders)
   ↓
2. Order Service:
   - Saves order to DB (pending status)
   - Emits "order.created" event to Kafka
   - Returns 202 Accepted
   ↓
3. Async Processing (in parallel):
   - Payment Service: Processes payment
   - Inventory Service: Reserves stock
   - Notification Service: Sends confirmations
   ↓
4. User polls /orders/{id} for status
   - Shows real-time status (pending → payment_approved → inventory_reserved)
\`\`\`

## Topics & Partitions

| Topic | Partitions | Purpose | Retention |
|-------|-----------|---------|-----------|
| orders | 3 | Order creation events | 7 days |
| payments | 3 | Payment processing | 7 days |
| inventory | 3 | Inventory reservations | 7 days |
| notifications | 1 | Email/notification events | 7 days |
| dlq | 1 | Dead-letter queue (errors) | 30 days |

Partition key: \`order_id\` → ensures same order always goes to same partition → maintains ordering

## Consumer Groups

| Service | Group | Topic | Parallelism |
|---------|-------|-------|-------------|
| Payment | payment-processor-group | payments | 3 (1 per partition) |
| Inventory | inventory-processor-group | inventory | 3 (1 per partition) |
| Notification | notification-processor-group | notifications | 1 |

## Error Handling

### Failure Scenarios

1. **Consumer crashes** → Kafka rebalances, another consumer picks up where failed one left off
2. **Duplicate message** → Processed event check (idempotent check) prevents duplicate processing
3. **Processing error** → Retries 3x, then sends to DLQ for manual review
4. **Database error** → Event not marked as processed, Kafka replay will retry

### Dead-Letter Queue (DLQ)

When consumer fails after 3 retries:
- Event sent to DLQ topic
- Logged with full context
- Manual intervention required
- Can replay after fix

## Performance Optimization

### Kafka Producer Config
- \`idempotent: true\` → No duplicate sends
- \`batch.size: 16384\` → 16KB batches for efficiency
- \`linger.ms: 10\` → Wait 10ms to batch messages
- \`compression: snappy\` → Reduce network overhead

### Database
- Connection pool: 20 connections
- Prepared statements for all queries
- Indexes on frequently queried columns

### Monitoring

Key metrics to track:
- Consumer lag per partition
- Event processing latency (P50, P95, P99)
- Error rate
- Throughput (events/sec)
```

---

## ✅ Checklist

- [ ] docker/Dockerfile criado
- [ ] docker-compose.yml criado
- [ ] scripts/init-db.sql criado
- [ ] docs/SETUP.md criado
- [ ] docs/ARCHITECTURE.md criado

**5 arquivos = PRONTO!**

---

## 📋 CHECKLIST FINAL DE TODOS ARQUIVOS

```
ROOT (7):
✅ .gitignore
✅ tsconfig.json
✅ jest.config.js
✅ .dockerignore
✅ LICENSE
✅ package.json
✅ .env.example

SRC (15):
✅ src/index.ts
✅ src/config.ts
✅ src/types.ts
✅ src/utils/logger.ts
✅ src/producers/base-producer.ts
✅ src/producers/order-producer.ts
✅ src/consumers/base-consumer.ts
✅ src/consumers/payment-consumer.ts
✅ src/consumers/inventory-consumer.ts
✅ src/consumers/notification-consumer.ts
✅ src/services/order-service.ts
✅ src/services/database.ts
✅ src/services/payment-service.ts
✅ src/services/inventory-service.ts
✅ src/services/notification-service.ts

DOCKER (2):
✅ docker/Dockerfile
✅ docker-compose.yml

SCRIPTS (1):
✅ scripts/init-db.sql

DOCS (2):
✅ docs/SETUP.md
✅ docs/ARCHITECTURE.md

README (1):
✅ README.md (arquivo separado com 1500+ words)

TOTAL: 23 arquivos ✅
```

---

## 🎉 PRONTO!

Você tem TODOS os 23 arquivos! 

Próximos passos:
1. Abra 02_GUIA_GITHUB.md
2. Siga passo-a-passo (criar no GitHub + commit + push)
3. Ready! 🚀
