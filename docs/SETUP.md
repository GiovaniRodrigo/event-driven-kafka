# Getting Started

## Prerequisites

- Node.js 18+ (tested on 20)
- Docker & Docker Compose v2
- Git

## Run everything with Docker (recommended)

```bash
docker compose up -d --build
```

This starts Zookeeper, Kafka, PostgreSQL, and the API (multi-stage build:
compiles TypeScript, then runs the compiled output). Wait for services to
become healthy:

```bash
docker compose ps
```

The API listens on `http://localhost:3000` (override the host port with
`APP_PORT=3001 docker compose up` if 3000 is already in use). Kafka topics
(`orders`, `payments`, `inventory`, `notifications`, `dlq`) are created by the
app on startup.

## Run the API locally against Docker infrastructure

```bash
npm install
docker compose up -d zookeeper kafka postgres
npm run dev   # or: npm run build && npm start
```

By default the app connects to `localhost:9092` (Kafka) and the discrete
`DB_*` variables. Inside Docker it uses `KAFKA_BROKER=kafka:29092` and a single
`DATABASE_URL`.

## Try it out

### Create an order

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_123",
    "items": [
      { "sku": "PROD_001", "name": "Sample Product", "price": 99.99, "quantity": 2 }
    ]
  }'
```

Response (`202 Accepted`):

```json
{ "order_id": "ord_abc123", "status": "pending", "message": "Order accepted. Processing asynchronously.", "created_at": "..." }
```

### Watch it advance through the pipeline

```bash
curl http://localhost:3000/orders/ord_abc123
```

The order status moves `pending -> payment_approved -> inventory_reserved ->
completed`, and the `events[]` array records its path across the Kafka topics
(`order.created`, `payment.approved`, `inventory.reserved`, `notification.sent`).

### List recent orders / health / metrics

```bash
curl http://localhost:3000/orders
curl http://localhost:3000/health     # includes per-consumer status
curl http://localhost:3000/metrics
```

## Common commands

```bash
npm run build          # compile TypeScript
npm run dev            # run with ts-node
docker compose logs -f app
docker compose down    # stop
docker compose down -v # stop and delete data
```
