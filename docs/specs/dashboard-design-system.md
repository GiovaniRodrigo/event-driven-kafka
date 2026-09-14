# Spec: Design System + Live Dashboard for the Event-Driven Kafka Order System

> Synthesized by `/to-spec`, incorporating the ~20 decisions settled in the `/grilling` design-tree session.
> **Publishing blocked:** the project's issue tracker and triage-label vocabulary are not configured. Run `/setup-matt-pocock-skills` to enable publishing this to the tracker with the `ready-for-agent` label.

## Problem Statement

This repository is a portfolio project meant to demonstrate an event-driven, Kafka-based order-processing system (orders flow asynchronously through Payment → Inventory → Notification consumers, backed by PostgreSQL). Today it has two problems, both from the owner's perspective:

1. **Nothing runs.** The entire backend — TypeScript source, `package.json`, `Dockerfile`, `docker-compose`, SQL — exists only as copy-paste snippets inside `docs/*.md`. There is no `src/`, nothing is deployed, and the "Deployed to production at Exbe" line in the template README is narrative, not fact. A reviewer cannot clone and run anything.
2. **Nothing is visible.** Even once the backend runs, an event-driven system is invisible: order lifecycle, the flow of events across Kafka topics, and consumer health can only be inferred from logs or manual API calls. That is not a compelling demonstration, and there is no reusable, documented UI foundation to build a demonstration on.

The owner needs the system to actually run, and needs a polished, reusable **design system** — the star deliverable — that both documents itself and powers a live view of the running system.

## Solution

Two coordinated tracks in this repo:

1. **Materialize the backend.** Move the code out of `docs/` into a runnable `src/` service (Node/Express/TypeScript + Kafka + PostgreSQL), refactoring for quality as it moves (module structure, typing, bug fixes), and add the read-side surface the UI needs (an orders list endpoint, per-order event history, per-consumer health, and a real-time event stream). The backend also runs a dedicated read-side Kafka consumer that bridges pipeline events to a WebSocket broadcast.

2. **Build the design system (the star) + a live dashboard.** Reimplement a component library from scratch (Vite + React + TypeScript on shadcn/ui + Radix + Tailwind), **faithfully reproducing the v0-generated design** — its app shell (left sidebar + operations top bar), screens, navigation, and features are the UI target; only v0's *code* is not adopted (it is rebuilt on this stack). The design system is documented as living documentation in a hosted **Storybook**, and is exercised for real by a multi-view, **bilingual (PT/EN)** dashboard that connects to the materialized backend over REST (SWR) plus a Socket.IO real-time layer. A shared, zod-based `contracts` package gives both sides a single, runtime-validated definition of every payload.

The result: a repo you can clone and `docker-compose up`, a public Storybook link for the design system, and a live dashboard that visibly shows orders moving through the Kafka pipeline.

## User Stories

### Backend materialization
1. As the project owner, I want the backend source extracted from `docs/` into a runnable `src/`, so that the repo can actually be cloned and started.
2. As a reviewer, I want to run the whole system with a single `docker-compose up` (API, Kafka, PostgreSQL), so that I can see it working without manual setup.
3. As the project owner, I want the code refactored for quality as it moves (clear modules, strong typing, fixed bugs) while staying on Node/Express + Kafka + PostgreSQL, so that the code reads as production-grade without being re-architected.
4. As an API consumer, I want `GET /orders` to return recent orders (id, user, status, total, created_at), so that a list view can be built.
5. As an API consumer, I want `GET /orders/:order_id` to include an `events[]` history (`{ type, topic, timestamp }`), so that an order's journey through the Kafka topics can be rendered.
6. As an API consumer, I want `GET /health` to report per-consumer status (payment, inventory, notification as healthy/degraded/down), so that consumer health can be shown.
7. As an API consumer, I want `POST /orders` to keep its existing semantics (202 Accepted on success, 400 on missing `user_id`/`items`), so that the create flow is unchanged.
8. As the project owner, I want a dedicated read-side consumer that subscribes to the Kafka result topics (OrderCreated, PaymentProcessed, InventoryReserved, NotificationSent) and broadcasts them over WebSocket, so that the web layer sees pipeline progress without touching the business consumers.
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

### App shell & internationalization (faithful to v0)
36. As an operator, I want the dashboard framed by a persistent **left sidebar** ("Order platform" workspace + primary navigation) alongside the top bar — not a top bar alone — so the shell matches the v0 layout and scales to more views.
37. As an operator, I want the top bar to carry the operational context the v0 shows — the **"Streamline / Kafka operations"** identity, an **environment badge** (e.g. `produção · us-east-1 / cluster-a`), a **search** entry point, a **profile** control, and the live-connection indicator — so the tool reads as a real operations console.
38. As a user, I want the entire interface available in **Portuguese and English** via an in-header language toggle that switches instantly without reload (nav, titles, descriptions, actions, labels, and `aria-label`s all translated), so the dashboard serves both audiences exactly as v0 does.
39. As an operator, I want the order flow to open the **orders list first and drill into a detail view via a breadcrumb** (list ⇄ detail), so navigation mirrors the v0 flow and the current view is always clear.

### Dashboard depth (faithful to v0)
40. As an operator, I want a **request/event map** view that visualizes requests across regions with routed connections, per-region volume, a live indicator, total requests and average latency, and a legend, so the flow of traffic is visible at a glance as in v0.
41. As an operator, I want each **node in the map to be selectable** (clickable and via a region dropdown), highlighting the selection and updating a detail panel with method, endpoint, origin, destination, status, latency, timestamp, and correlation id, so I can inspect an individual request as in v0.
42. As an operator, I want an **AI analysis** field on the order-detail view showing an automatic summary of the order's events with a confidence indicator and a visual confidence bar (read-only, translated, accessible), so I get a plain-language read on each order as in v0.
43. As an operator, I want the order detail to expose the **raw event payload as formatted JSON**, so I can inspect the exact message that moved through the pipeline, matching v0.
44. As an operator, I want the metric surface to include **throughput, consumer lag, and an event-flow view** (not only aggregate counts), each MetricCard carrying a value, delta, and trend sparkline, so operational direction is visible as in v0.

## Implementation Decisions

**Repository structure**
- **pnpm workspaces** with separate packages: the backend service, the `web` Vite app, and a shared **`contracts`** package. No Nx/Turborepo.
- `contracts` holds **zod schemas** for every REST response and WebSocket message; TS types are inferred from them and imported by both backend and web. Both sides validate at their boundaries.

**Backend**
- Kept on **Node/Express/TypeScript + Kafka + PostgreSQL** (as designed in `docs/`); code is materialized from `docs/*.md` into `src/` and refactored for quality during the move. Scope of refactor is **bounded** — structure, typing, bug fixes, and the new endpoints — not a re-architecture and not a persistence swap.
- **New/extended REST surface:** `GET /orders` (list), `GET /orders/:order_id` extended with `events[]` (`{ type, topic, timestamp }`), `GET /health` extended with per-consumer status. `POST /orders` semantics unchanged (202 / 400).
- **Real-time via Socket.IO.** A dedicated **read-side consumer** subscribes to the Kafka result topics (OrderCreated, PaymentProcessed, InventoryReserved, NotificationSent) and broadcasts to Socket.IO clients. The business consumers (payment/inventory/notification) are untouched by the web layer.
- **Socket.IO channel model (hybrid):** on connect, clients receive a **global broadcast** of `order:created`, `order:updated` (status change), and `consumer:health`. On opening an order detail, a client `subscribe`s to a **per-order room** `order:<id>` and receives `order:event` (`{ order_id, type, topic, timestamp }`); it leaves the room on navigating away. All event payloads are zod schemas in `contracts`.

**Frontend**
- **Vite + React + TypeScript** SPA (no Next.js; the backend is the only server). **react-router-dom** for the multi-view routing.
- Component library reimplemented from scratch on **shadcn/ui (Radix + Tailwind)**. The v0 design (`v0.app/giovanirodrigos-projects/chat/design-system-kafka-order-dashboard-mBDo5NwtERV`) is the **UI source of truth, reproduced faithfully** — its layout, app shell, screens, and features are the target; only v0's *code* is not adopted (rebuilt on this stack instead of importing v0's Next.js output).
- **App shell (faithful to v0):** a persistent **left sidebar** ("Order platform" workspace + primary navigation) plus a top bar carrying the **"Streamline / Kafka operations"** identity, an **environment badge** (`produção · us-east-1 / cluster-a`), **search**, **profile**, the **language toggle**, and the live-connection indicator.
- **Internationalization:** **Portuguese + English**, toggled in-header and switching instantly without reload; every user-facing string and `aria-label` is translated. Lightweight in-app i18n (no server, no persistence beyond the session).
- **Design tokens** as CSS variables / Tailwind theme; dark-first with a working light theme.
- **Data layer:** **SWR** for REST reads; the Socket.IO subscription patches the SWR cache on incoming messages via `mutate(key, updater, { revalidate: false })`, so the SWR cache is the single source of truth and real-time only updates it.
- **Dashboard surface (faithful to v0, multi-view):**
  - **Overview** — MetricCards (value + delta + trend sparkline, including throughput and consumer lag), OrdersTable, consumer HealthPill panel, and the **request/event map**.
  - **Orders** — the **list first**, drilling into an **order detail** via a breadcrumb; the detail shows the summary, line items, the **EventTimeline** (live via its per-order room), the **AI analysis** field (summary + confidence + visual bar), and the **raw JSON payload**.
  - **Consumers/health** — per-consumer HealthPill board with healthy/degraded/down states.
  - **Request map** — region view with routed connections, per-region volume, live indicator, totals + average latency, and a legend; **selectable nodes** (click or region dropdown) whose detail panel shows method, endpoint, origin, destination, status, latency, timestamp, and correlation id.
  - A **create-order** form triggers the pipeline for demos.

**Documentation**
- The design system's living documentation is **Storybook 8 (Vite builder)**, hosted on **GitHub Pages via GitHub Actions on push to `main`**. Storybook renders themed. There is no separate hand-built style-guide page.

## Testing Decisions

- **What makes a good test here:** it asserts external, user-observable behavior through the highest available seam, never implementation details (no assertions on class names, props, or internal cache calls). A frontend test asserts what a user sees; a backend test asserts what a client observes over HTTP or the socket.
- **Frontend — one seam, both network boundaries mocked.** Render a view (primarily the Overview, plus the order-detail flow) with REST faked at the HTTP layer (MSW) and the Socket.IO client faked, then assert behavior: rows appear with the correct StatusBadge, a pushed `order:updated` event updates the row, a pushed `order:event` updates the EventTimeline on the detail view, a 400 shows form validation, a 202 shows a success toast, and an unreachable API shows the error state. Component-level behavior is exercised transitively through these view tests (the components are presentational); Storybook interaction tests may cover component states but do not replace the view seam.
- **Backend — two seams.** (1) The **HTTP endpoints**, tested against the Express app (supertest-style) with a test database or repository, asserting the response contracts for `GET /orders`, `GET /orders/:id` (including `events[]`), `GET /health`, and `POST /orders` (202/400). (2) The **Kafka→WebSocket bridge**, tested by feeding a fake Kafka result message into the read-side consumer and asserting the corresponding Socket.IO broadcast (correct event name, room, and zod-valid payload).
- **Contracts** are validated implicitly everywhere: because payloads are parsed with the shared zod schemas at the boundaries, a contract drift surfaces as a boundary failure in the above seams.
- **Modules under test:** the dashboard views/flows; the backend read endpoints; the Kafka→WS bridge. Not each design-system component in isolation beyond Storybook stories.
- **Prior art:** the repo is greenfield with no existing tests, so this establishes the pattern. Test runners follow the stack: **Vitest** for the Vite web app and (by default) the backend; MSW for HTTP mocking; supertest for the Express app.

## Out of Scope

- Deploying the full application (API, Kafka, PostgreSQL, web app) to a hosted environment — it runs locally via `docker-compose`; only Storybook is publicly hosted.
- Authentication / authorization and multi-tenant access control.
- A metrics/time-series backend (Prometheus-style history); MetricCards — including throughput, consumer lag, and trend sparklines — render from what the API provides plus derived/representative values where a historical series is unavailable.
- A geospatial/telemetry backend for the **request map** (regions, routes, per-request correlation-id latency) and a real ML service behind the **AI-analysis** field; where the materialized backend does not expose this data, these views render from derived/representative data client-side, reproducing the v0 visuals without adding new backend infrastructure.
- Re-architecting the Kafka topology or the business logic of the payment/inventory/notification consumers (beyond the read-side consumer and the new read endpoints), and any persistence swap away from PostgreSQL.
- Editing or cancelling orders from the dashboard (read + create only).
- Adopting the v0-generated *code* (its Next.js output). The v0 **design** is the UI source of truth and is reproduced faithfully, but it is reimplemented from scratch on the shadcn/ui stack rather than imported.
- Persisting UI preferences (theme, filters) beyond a single session.
- TanStack Router / TanStack Query, SSE, and a hand-built style-guide page — all considered and rejected in favor of react-router-dom, SWR, Socket.IO, and Storybook respectively.

## Further Notes

- **Design-tree provenance:** the ~20 decisions above were settled interactively (`/grilling`), reversing several initial recommendations — notably reimplement-from-scratch (over adopting v0), connect-to-real-backend (over standalone docs), WebSocket/Socket.IO (over SSE), SWR (over React Query), and Storybook (over a custom style-guide page).
- **v0 design (UI source of truth, reproduced faithfully):** chat `mBDo5NwtERV` (`https://v0.app/giovanirodrigos-projects/chat/design-system-kafka-order-dashboard-mBDo5NwtERV`), preview `https://design-system-kafka-order-dashboard.v0.build`. Both require the owner's interactive v0 login in the browser; the chat is also reachable programmatically through the connected **v0 MCP connector** (`listChats`/`getChat`/`getPreview`/`listMessages`), though the connector returns chat metadata and the build log — not the rendered pixels or the generated source, which live in the v0 VM. The v0 build produced these screens the implementation must reproduce: operational **overview** (with the request map), **orders list → order detail**, **consumers/health**, the **AI-analysis** field on the order detail, the **request map** plus its selected-node state, and full **PT/EN** localization. App-shell chrome: left sidebar (WORKSPACE › "Order platform" › primary nav incl. Métricas, Tópicos Kafka) + top bar with the "Streamline / Kafka operations" brand, environment badge, search, profile, and language toggle; footer "Design system v2.4.0".
- **Design-fidelity revision:** this spec was updated so the v0 design is the **faithful UI target** — adding the sidebar app shell, PT/EN i18n, the request map (with node selection), the AI-analysis field, the JSON payload view, and throughput/lag metrics as first-class requirements (user stories 36–44). The earlier "v0 output is visual reference only" framing is superseded; "reimplement from scratch" still holds for v0's *code*, not its design.
- **Critical path:** the three new read endpoints plus the Kafka→WS bridge are the dependency that makes the live dashboard truthful; the design system and Storybook can proceed in parallel against mock args.
- **Repo hygiene:** the git remote is registered as `orign` (typo for `origin`) pointing at `github.com/GiovaniRodrigo/event-driven-kafka` — unrelated to this feature but worth fixing.
- **Sequencing suggestion (not binding):** (1) workspaces + `contracts` skeleton; (2) materialize backend + new endpoints + read-side bridge; (3) design system + Storybook; (4) wire the multi-view dashboard to REST + Socket.IO; (5) tests at the seams above; (6) README + hosted Storybook.

## Known Defects in the Received Backend Snippets

A Sourcery review of PR #1 validated the following defects in the copy-paste backend code under `docs/`. These are the concrete bug list for the **materialization track** (fix them as the code moves to `src/`, where it can be built and tested). Items marked *fixed* were corrected in-place in the docs because they were localized and verifiable; the rest are deferred because they require a runnable build to fix and verify safely.

- **Kafka topic topology is disconnected** *(deferred)* — the producer emits `order.created` to `orders`, but the payment/inventory/notification consumers subscribe to result topics no producer emits, so the pipeline never advances. Define the topic/event topology so each stage emits to the topic the next stage consumes.
- **Order producer never connects** *(deferred)* — `producer.connect()` is not called at startup, so the first `POST /orders` hits the 500 path. Manage the producer lifecycle (connect on start, disconnect on shutdown).
- **Order items are dropped** *(deferred)* — `insertOrder` never writes `items` to `metadata`, while `getOrder` reads items only from `metadata.items`, so fetched orders return `items: []`. Persist items (in `metadata` or a dedicated table).
- **Status-transition metadata lost on NULL** *(fixed)* — `metadata = metadata || $2` yields NULL when metadata is initially NULL; changed to `COALESCE(metadata, '{}'::jsonb) || $2`.
- **Dockerfile / compose cannot build or run** *(deferred)* — `npm ci` needs a committed lockfile, `COPY dist/` assumes a build that never runs, and compose runs `npm run dev` without dev deps/`ts-node`. Use a multi-stage build (install dev deps, run `tsc`), ship the lockfile, and run a command compatible with the installed deps.
- **`DATABASE_URL` ignored by config** *(deferred)* — compose supplies only `DATABASE_URL`, but `databaseConfig` reads `DB_*` with a `localhost` fallback, so DB init fails in-container. Parse `DATABASE_URL` or pass the discrete `DB_*` vars.
- **`db:migrate` points at a missing file** *(fixed)* — the script invoked `scripts/migrate.js`, which is not provided; changed to run the supplied `scripts/init-db.sql` via `psql`.
- **Strict tsconfig breaks the build** *(fixed)* — `noUnusedLocals`/`noUnusedParameters` flagged an unused `PoolClient` import and unused Express `req` params; removed the unused import and relaxed `noUnusedParameters` (idiomatic unused handler params) while keeping `noUnusedLocals`. Materialization can re-enable strict param checks with `_`-prefixed params.
- **DLQ failure is swallowed** *(deferred)* — after the retry limit, `handleError` swallows DLQ connect/send failures and returns normally, letting Kafka commit the source offset and permanently lose the event. Propagate the DLQ failure so the offset is not committed, or persist the failed event durably first.
- **Stale file counts in the guides** *(deferred, nitpick)* — the guides claim 23 files while enumerating different totals and referencing former filenames; regenerate counts from the actual tree.
