# Event-Driven Kafka Order System

An event-driven, Kafka-based order-processing system with a **from-scratch design
system** and a **live dashboard** that shows orders moving through the pipeline in
real time.

Orders flow asynchronously through **Payment → Inventory → Notification** consumers
over Kafka, backed by PostgreSQL. A dedicated read-side consumer bridges pipeline
events to a Socket.IO broadcast, and a Vite/React dashboard — built on an in-house
component library — renders it all live.

## Repository layout

This is a **pnpm workspace** with three packages:

| Package | Path | What it is |
| --- | --- | --- |
| `@kafka-demo/backend` | [`packages/backend`](packages/backend) | Node/Express + KafkaJS + PostgreSQL service (producers, consumers, REST, read-side Socket.IO bridge). |
| `@kafka-demo/web` | [`packages/web`](packages/web) | The design system + the live dashboard (Vite + React + TS, Tailwind + Radix), documented in Storybook. |
| `@kafka-demo/contracts` | [`packages/contracts`](packages/contracts) | Shared **zod** schemas for every REST and Socket.IO payload; both sides infer types from them and validate at the boundary. |

## Running the system

### Full stack (Docker)

Brings up Kafka, ZooKeeper, PostgreSQL, and the API:

```bash
docker compose up
```

The API is then on `http://localhost:3000` (override the host port with
`APP_PORT`). See [docs/SETUP.md](docs/SETUP.md) for details.

### The dashboard (dev)

With the backend running, start the web app (it proxies `/api` and `/socket.io`
to `:3000`):

```bash
pnpm install
pnpm --filter @kafka-demo/web dev
```

Then open `http://localhost:5173`. Create a test order from the UI to watch it
flow through the pipeline end to end.

## Design system + Storybook

The component library (Button, StatusBadge, HealthPill, MetricCard,
EventTimeline, OrdersTable, Alert, Input, Select, Toast, …) is dark-first with a
working light theme, built on accessible Radix primitives and semantic design
tokens. Its living documentation is **Storybook**, hosted on GitHub Pages:

- **Storybook:** https://giovanirodrigo.github.io/event-driven-kafka/ _(published by CI on push to `main`)_

Run it locally:

```bash
pnpm --filter @kafka-demo/web storybook
```

## REST API

| Method | Route | Description |
| --- | --- | --- |
| `POST` | `/orders` | Create an order (`202 Accepted`; `400` on missing `user_id`/`items`). |
| `GET` | `/orders` | Recent orders. |
| `GET` | `/orders/:id` | One order with its `events[]` history. |
| `GET` | `/health` | Service + per-consumer health. |
| `GET` | `/metrics` | Aggregate order metrics. |

Real time (Socket.IO): a global broadcast of `order:created`, `order:updated`,
and `consumer:health`; a per-order room (`order:<id>`) streams `order:event` for
the detail view.

## Tests

```bash
pnpm -r test
```

- **Backend** — HTTP endpoints (supertest) and the Kafka→WebSocket bridge.
- **Web** — view-seam tests (Vitest + Testing Library) with REST faked via MSW
  and the Socket.IO client faked, asserting what a user observes.

## Build

```bash
pnpm -r build
```

Builds `contracts` → `backend` → `web` in dependency order.
