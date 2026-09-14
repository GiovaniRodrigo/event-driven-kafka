# Spec: Design System + Live Dashboard for the Event-Driven Kafka Order System

Status: ready-for-agent

> Published to the local-markdown issue tracker (`.scratch/`) per `docs/agents/issue-tracker.md`.
> Provenance: synthesized by `/to-spec`, incorporating the ~20 decisions settled in the `/grilling` design-tree session. Originally blocked on tracker setup; that setup is now complete.

## Current State (read this first)

Since this spec was first drafted, **the backend materialization track (Track 1) has been delivered.** The system now runs from a real `src/` service, not from copy-paste snippets in `docs/`:

- `src/` contains the Express app, config, the three business consumers (payment/inventory/notification) on a shared base consumer, the order producer, the services (order/payment/inventory/notification/database), and the zod `contracts`.
- The read-side surface exists: `GET /orders`, `GET /orders/:order_id` (with `events[]`), `GET /health` (per-consumer), `GET /metrics`, and `POST /orders` (202/400 unchanged).
- The Kafka→WebSocket bridge exists: a dedicated read-side consumer (`src/realtime/realtime-consumer.ts`) broadcasts through a `RealtimeGateway` abstraction (`src/realtime/realtime-gateway.ts`, Socket.IO implementation), with global broadcasts + per-order rooms.
- Backend tests exist at the seams below (`tests/http/orders-api.test.ts`, `tests/realtime/*.test.ts`).
- The topology, producer lifecycle, item persistence, `COALESCE` metadata merge, and DLQ-offset behavior described in `docs/ARCHITECTURE.md` reflect the fixes to the defects listed at the bottom of this spec.

**Track 1 is retained below as a record of what was built and why.** The actionable, not-yet-built work is **Track 2: the design system (the star), Storybook, the live dashboard, and the pnpm-workspaces restructure** (contracts is currently `src/contracts/`, not yet a standalone shared package; there is no web app and no Storybook).

## Problem Statement

This repository is a portfolio project meant to demonstrate an event-driven, Kafka-based order-processing system (orders flow asynchronously through Payment → Inventory → Notification consumers, backed by PostgreSQL). It began with two problems, both from the owner's perspective:

1. **Nothing ran.** The entire backend existed only as copy-paste snippets inside `docs/*.md`. A reviewer could not clone and run anything. _(Resolved: the backend has since been materialized into a runnable `src/` service — see Current State.)_
2. **Nothing is visible.** An event-driven system is invisible: order lifecycle, the flow of events across Kafka topics, and consumer health can only be inferred from logs or manual API calls. That is not a compelling demonstration, and there is still no reusable, documented UI foundation to build a demonstration on. _(Open — this is what Track 2 addresses.)_

The owner needs a polished, reusable **design system** — the star deliverable — that both documents itself and powers a live view of the running system.

## Solution

Two coordinated tracks in this repo:

1. **Materialize the backend.** _(Delivered.)_ Move the code out of `docs/` into a runnable `src/` service (Node/Express/TypeScript + Kafka + PostgreSQL), refactoring for quality as it moves (module structure, typing, bug fixes), and add the read-side surface the UI needs (an orders list endpoint, per-order event history, per-consumer health, and a real-time event stream). The backend also runs a dedicated read-side Kafka consumer that bridges pipeline events to a WebSocket broadcast.

2. **Build the design system (the star) + a live dashboard.** _(Not started.)_ Reimplement a component library from scratch (Vite + React + TypeScript on shadcn/ui + Radix + Tailwind), using the v0-generated output only as a visual reference. The design system is documented as living documentation in a hosted **Storybook**, and is exercised for real by a small multi-view dashboard that connects to the materialized backend over REST (SWR) plus a Socket.IO real-time layer. A shared, zod-based `contracts` package gives both sides a single, runtime-validated definition of every payload.

The result: a repo you can clone and `docker-compose up`, a public Storybook link for the design system, and a live dashboard that visibly shows orders moving through the Kafka pipeline.

## User Stories

### Backend materialization (delivered — retained as record)
1. As the project owner, I want the backend source extracted from `docs/` into a runnable `src/`, so that the repo can actually be cloned and started.
2. As a reviewer, I want to run the whole system with a single `docker-compose up` (API, Kafka, PostgreSQL), so that I can see it working without manual setup.
3. As the project owner, I want the code refactored for quality as it moves (clear modules, strong typing, fixed bugs) while staying on Node/Express + Kafka + PostgreSQL, so that the code reads as production-grade without being re-architected.
4. As an API consumer, I want `GET /orders` to return recent orders (id, user, status, total, created_at), so that a list view can be built.
5. As an API consumer, I want `GET /orders/:order_id` to include an `events[]` history (`{ type, topic, timestamp }`), so that an order's journey through the Kafka topics can be rendered.
6. As an API consumer, I want `GET /health` to report per-consumer status (payment, inventory, notification as healthy/degraded/down), so that consumer health can be shown.
7. As an API consumer, I want `POST /orders` to keep its existing semantics (202 Accepted on success, 400 on missing `user_id`/`items`), so that the create flow is unchanged.
8. As the project owner, I want a dedicated read-side consumer that subscribes to the Kafka result topics and broadcasts them over WebSocket, so that the web layer sees pipeline progress without touching the business consumers.
9. As a developer, I want the business consumers (payment/inventory/notification) to remain unaware of the web transport, so that the read side is decoupled (CQRS-flavored).

### Design system (the star)
10. As a developer, I want a from-scratch component library on shadcn/ui + Radix + Tailwind, so that I own the code and get accessible headless primitives.
11. As a developer, I want semantic color tokens (primary, success, warning, danger, info, neutral/surface/background) as CSS variables / Tailwind theme, so that theming is centralized.
12. As a developer, I want a typographic scale (display → caption plus a mono family for IDs/logs), so that identifiers and log-like data read correctly.
13. As a developer, I want reusable components — Button, StatusBadge, HealthPill, MetricCard, EventTimeline, OrdersTable, Toast/Alert, Input/Select — each with its variants and states, so that views are composed, not copy-pasted.
14. As a developer, I want a dark-first theme with a working light theme, so that the aesthetic matches an observability tool without breaking in light.
15. As a developer, I want every component accessible (contrast, visible focus, appropriate ARIA), so that the design system is usable by keyboard and screen reader.

### Living documentation (Storybook)
16. As a developer, I want each component documented in Storybook with stories for its states (using mock args), so that the design system is the discoverable, isolated source of truth.
17. As the project owner, I want Storybook built and hosted on GitHub Pages via CI on push to `main`, so that recruiters have a public link to the design system.
18. As a reviewer, I want Storybook to render themed (dark/light), so that components look as intended in isolation.

### Live dashboard (exercises the design system)
19. As an operator, I want an Overview screen assembling MetricCards, the OrdersTable, and a consumer HealthPill panel with live data, so that I get an at-a-glance operational picture.
20. As an operator, I want each order's status rendered as a semantic StatusBadge (pending, paid, shipped, delivered, failed/cancelled), so that I can scan states quickly.
21. As an operator, I want Order IDs shown in the mono type style, so that identifiers are unambiguous and copy-friendly.
22. As an operator, I want an order-detail view with an EventTimeline of that order's path through the Kafka topics with timestamps, so that I can trace how far it progressed and reason about latency.
23. As an operator, I want a consumers/health view showing each consumer's HealthPill, so that I can spot a stuck part of the pipeline.
24. As an operator, I want MetricCards (e.g. orders/min, consumer lag, events processed) with a value, delta, and trend sparkline, so that I see direction, not just a number.
25. As an operator, I want the Overview and health views to update live via the global WebSocket broadcast (order created/updated, consumer health), so that I watch the system without reloading.
26. As an operator, I want the order-detail view to subscribe to that order's room and update its timeline live as new pipeline events arrive, so that I see progress in real time on the record I'm looking at.
27. As a user, I want to submit a test order from the dashboard, so that I can trigger the pipeline and watch it flow end to end during a demo.
28. As a user, I want validation feedback on the order form (missing user/items) mirroring the API's 400, so that I understand why a submission was rejected.
29. As a user, I want a success toast on 202 and an error toast/alert on failure, so that I get clear feedback on outcomes.
30. As an operator, I want failed/cancelled orders visibly marked as failed in the table and timeline, so that unhappy paths are as legible as happy ones.
31. As a user, I want clear loading, empty, and API-unreachable states across views, so that "no data" is distinguishable from "backend down" and the UI never flashes empty.
32. As a user on mobile, I want the dashboard responsive, so that it is usable and demoable on a phone.

### Cross-cutting
33. As a developer, I want a shared `contracts` package of zod schemas defining every REST and WebSocket payload, inferring the TS types, so that both sides validate at the boundary from a single source.
34. As a developer, I want the repo organized as pnpm workspaces (backend, web app, contracts), so that types are shared type-safely without duplication or heavy monorepo tooling.
35. As the project owner, I want the README to explain and link the running system and the hosted Storybook, so that a visitor knows what exists and how to run it.

## Implementation Decisions

**Repository structure**
- **pnpm workspaces** with separate packages: the backend service, the `web` Vite app, and a shared **`contracts`** package. No Nx/Turborepo. _(The zod contracts currently live at `src/contracts/`; extracting them into a standalone workspace package consumed by both backend and web is part of this track.)_
- `contracts` holds **zod schemas** for every REST response and WebSocket message; TS types are inferred from them and imported by both backend and web. Both sides validate at their boundaries.

**Backend** _(delivered — decisions retained as record)_
- Kept on **Node/Express/TypeScript + Kafka + PostgreSQL**; code materialized from `docs/*.md` into `src/` and refactored for quality during the move. Scope of refactor **bounded** — structure, typing, bug fixes, and the new endpoints — not a re-architecture and not a persistence swap.
- **New/extended REST surface:** `GET /orders` (list), `GET /orders/:order_id` extended with `events[]` (`{ type, topic, timestamp }`), `GET /health` extended with per-consumer status, `GET /metrics`. `POST /orders` semantics unchanged (202 / 400).
- **Real-time via Socket.IO.** A dedicated **read-side consumer** subscribes to the Kafka result topics and broadcasts to Socket.IO clients. The business consumers (payment/inventory/notification) are untouched by the web layer.
- **Socket.IO channel model (hybrid):** on connect, clients receive a **global broadcast** of `order:created`, `order:updated` (status change), and `consumer:health`. On opening an order detail, a client `subscribe`s to a **per-order room** `order:<id>` and receives `order:event` (`{ order_id, type, topic, timestamp }`); it leaves the room on navigating away. All event payloads are zod schemas in `contracts`.

**Frontend** _(to build)_
- **Vite + React + TypeScript** SPA (no Next.js; the backend is the only server). **react-router-dom** for the multi-view routing.
- Component library reimplemented from scratch on **shadcn/ui (Radix + Tailwind)**; the v0 output is visual reference only, not adopted code.
- **Design tokens** as CSS variables / Tailwind theme; dark-first with a working light theme.
- **Data layer:** **SWR** for REST reads; the Socket.IO subscription patches the SWR cache on incoming messages via `mutate(key, updater, { revalidate: false })`, so the SWR cache is the single source of truth and real-time only updates it.
- **Dashboard surface (multi-view):** an **Overview** page (MetricCards row + OrdersTable + consumer HealthPill panel), an **order-detail** route (EventTimeline for the selected order, live via its room), and a **consumers/health** route. A create-order form triggers the pipeline for demos.

**Documentation**
- The design system's living documentation is **Storybook 8 (Vite builder)**, hosted on **GitHub Pages via GitHub Actions on push to `main`**. Storybook renders themed. There is no separate hand-built style-guide page.

## Testing Decisions

Tests follow this project's **testing pyramid** (per the `teste-expert` convention): a broad base of fast atomic/unit tests, a middle layer of integration tests at the network boundaries, and a thin top of end-to-end tests. Across all layers, a good test asserts **external, user-observable behavior**, never implementation details — no assertions on class names, internal props, or which cache function was called. A frontend test asserts what a user sees; a backend test asserts what a client observes over HTTP or the socket.

**Layer 1 — Atomic / unit (broad base)**
- **Design-system components in isolation** (React Testing Library): each component's variants and states render and behave as specified — StatusBadge maps each order status to the correct semantic variant and label; HealthPill maps healthy/degraded/down; MetricCard renders value, delta sign, and sparkline; EventTimeline orders events and renders timestamps; Button/Input/Select states (disabled, loading, error). Assert rendered output and accessible roles/labels, not internal structure.
- **Pure logic units:** the SWR-cache patch updaters (given a cached list/detail and an incoming `order:created`/`order:updated`/`order:event`, produce the correct next cache value) and zod schema parsing (valid payloads parse, malformed payloads reject) tested as plain functions with no React or network.

**Layer 2 — Integration (middle)**
- **Frontend view integration — one seam, both network boundaries faked.** Render a whole view (primarily the Overview, plus the order-detail flow) with REST faked at the HTTP layer (MSW) and the Socket.IO client faked, then assert behavior: rows appear with the correct StatusBadge; a pushed `order:updated` updates the row; a pushed `order:event` updates the EventTimeline on the detail view; a 400 shows form validation; a 202 shows a success toast; an unreachable API shows the error state. This wires components + router + data layer together through their real integration, without a real backend.
- **Backend HTTP integration** _(exists)_: the Express app tested with supertest against a test database/repository, asserting the response contracts for `GET /orders`, `GET /orders/:id` (including `events[]`), `GET /health`, `GET /metrics`, and `POST /orders` (202/400).
- **Backend Kafka→WebSocket bridge integration** _(exists)_: feed a fake Kafka result message into the read-side consumer and assert the corresponding Socket.IO broadcast (correct event name, room, and zod-valid payload) via the `RealtimeGateway` abstraction.

**Layer 3 — End-to-end (thin top)**
- **Demo happy-path E2E** (Playwright against the running `docker-compose` stack): submit a test order from the dashboard and assert it appears in the OrdersTable, its status advances through the pipeline live, and its EventTimeline fills in on the detail view — the full "watch an order flow end to end" demo path. Kept deliberately thin (one or two flows); the pyramid's base and middle carry the coverage.

**Contracts** are validated implicitly at every layer: because payloads are parsed with the shared zod schemas at the boundaries, contract drift surfaces as a failure in the layer that crosses that boundary.

**Modules under test:** design-system components (Layer 1); cache/contract logic (Layer 1); dashboard views/flows (Layer 2); backend read endpoints and the Kafka→WS bridge (Layer 2, already present); the end-to-end demo path (Layer 3).

**Prior art:** the backend integration seams already exist in `tests/http/orders-api.test.ts` and `tests/realtime/*.test.ts` — mirror their style for new backend tests. The frontend layers are greenfield and establish the pattern. Test runners follow the stack: **Vitest** for the Vite web app; MSW for HTTP mocking; **Playwright** for E2E; the backend keeps its existing Jest + supertest setup.

## Out of Scope

- Deploying the full application (API, Kafka, PostgreSQL, web app) to a hosted environment — it runs locally via `docker-compose`; only Storybook is publicly hosted.
- Authentication / authorization and multi-tenant access control.
- A metrics/time-series backend (Prometheus-style history); MetricCards render what the API provides, sparklines from available data.
- Re-architecting the Kafka topology or the business logic of the payment/inventory/notification consumers (beyond the read-side consumer and the read endpoints), and any persistence swap away from PostgreSQL.
- Editing or cancelling orders from the dashboard (read + create only).
- Adopting the v0-generated code as source of truth (it is visual reference only).
- Persisting UI preferences (theme, filters) beyond a single session.
- TanStack Router / TanStack Query, SSE, and a hand-built style-guide page — all considered and rejected in favor of react-router-dom, SWR, Socket.IO, and Storybook respectively.

## Further Notes

- **Design-tree provenance:** the ~20 decisions above were settled interactively (`/grilling`), reversing several initial recommendations — notably reimplement-from-scratch (over adopting v0), connect-to-real-backend (over standalone docs), WebSocket/Socket.IO (over SSE), SWR (over React Query), and Storybook (over a custom style-guide page).
- **v0 reference:** chat `https://v0.app/giovanirodrigos-projects/chat/design-system-kafka-order-dashboard-mBDo5NwtERV`, preview `https://design-system-kafka-order-dashboard.v0.build` (both require the owner's v0 login).
- **Critical path:** the three read endpoints plus the Kafka→WS bridge — the dependency that makes the live dashboard truthful — are **already delivered**, so the design system, Storybook, and dashboard can now proceed directly against the running backend (or against mock args for isolated component work).
- **Sequencing suggestion (not binding):** (1) pnpm workspaces + extract `contracts` into a shared package; (2) design system + Storybook (against mock args); (3) wire the multi-view dashboard to REST + Socket.IO; (4) tests across the three pyramid layers; (5) README + hosted Storybook.

## Known Defects in the Original Backend Snippets (addressed during materialization)

A Sourcery review of PR #1 validated the following defects in the original copy-paste backend code under `docs/`. They were the concrete bug list for the materialization track and were addressed as the code moved to `src/`; `docs/ARCHITECTURE.md` documents the resulting, corrected design. Retained here as a record; verify each still holds when touching the backend.

- **Kafka topic topology is disconnected** — each stage must emit to the topic the next stage consumes (orders → payments → inventory → notifications). _(Addressed; see ARCHITECTURE topics table.)_
- **Order producer never connects** — the producer lifecycle must connect on start and disconnect on shutdown. _(Addressed.)_
- **Order items are dropped** — items must be persisted (in `metadata` or a dedicated table) and read back consistently. _(Addressed via `metadata`.)_
- **Status-transition metadata lost on NULL** — use `COALESCE(metadata, '{}'::jsonb) || $2` so nothing is dropped on NULL metadata. _(Fixed.)_
- **Dockerfile / compose cannot build or run** — multi-stage build (install dev deps, run `tsc`), ship the lockfile, run a command compatible with installed deps. _(Addressed.)_
- **`DATABASE_URL` ignored by config** — parse `DATABASE_URL` or pass discrete `DB_*` vars so DB init works in-container. _(Addressed; `DATABASE_URL` preferred.)_
- **`db:migrate` points at a missing file** — run the supplied `scripts/init-db.sql` via `psql`. _(Fixed.)_
- **Strict tsconfig breaks the build** — resolve unused-import/param strictness; `_`-prefix idiomatic unused handler params. _(Fixed.)_
- **DLQ failure is swallowed** — propagate a DLQ connect/send failure so the source offset is not committed and the event is redelivered rather than lost. _(Addressed; see ARCHITECTURE reliability.)_
- **Stale file counts in the guides** — regenerate counts from the actual tree. _(Nitpick; verify.)_

## Comments

_(none yet)_
