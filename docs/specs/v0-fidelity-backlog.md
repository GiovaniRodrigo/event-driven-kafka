# Backlog: v0 Design Fidelity

Gaps between the **implemented frontend** (`packages/web/`, branch `claude/implementation-status-892cde`) and the **revised spec** ([dashboard-design-system.md](./dashboard-design-system.md)), which now treats the v0 design as the faithful UI target. Each item maps to the spec's user stories (36–44) added in the fidelity revision.

The v0 build produced these screens the implementation must reproduce: operational **overview** (with request map), **orders list → order detail**, **consumers/health**, **AI-analysis** on the order detail, the **request map** (+ selected-node state), and full **PT/EN** localization — inside an app shell with a left sidebar and an operations top bar (brand "Streamline / Kafka operations", environment badge, search, profile, language toggle; footer "Design system v2.4.0").

| # | Gap | Spec stories | Current state |
|---|-----|--------------|---------------|
| 1 | Left **sidebar** app shell | 36 | Top-bar-only nav (`routes/AppLayout.tsx`) |
| 2 | Operations **top-bar chrome** (brand, env badge, search, profile) | 37 | Minimal top bar (title + Live pill + theme + New order) |
| 3 | **PT/EN i18n** | 38 | English-only, no i18n layer |
| 4 | **Orders list → detail** with breadcrumb | 39 | Overview table → detail with a "Back to overview" link |
| 5 | **Request map** view | 40 | Absent |
| 6 | Map **node selection** + detail panel | 41 | Absent |
| 7 | **AI analysis** field on order detail | 42 | Absent |
| 8 | **JSON payload** viewer on order detail | 43 | Absent |
| 9 | **Throughput/lag + event-flow** metrics (delta + sparkline) | 44 | Static counts; MetricCard supports delta/sparkline but Overview doesn't pass them |

---

## Items

### 1. App shell: left sidebar (story 36)
Add a persistent left sidebar ("WORKSPACE › Order platform" + primary navigation) alongside the top bar, matching v0. Nav items include at least Overview/Métricas, Orders, Consumers/Tópicos Kafka, and Request map. Responsive: collapses on mobile.
**Done when:** sidebar renders on every route, is keyboard-accessible, highlights the active route, and collapses/《drawer》 on small screens.

### 2. Operations top-bar chrome (story 37)
Extend the top bar to carry the v0 operations context: "Streamline / Kafka operations" identity, an environment badge (`produção · us-east-1 / cluster-a`), a search entry point, a profile control, plus the existing live indicator, theme, and language toggle. Footer shows "Design system v2.4.0".
**Done when:** all elements render, search opens (even if stubbed), profile is a menu, and the environment badge is data-driven or configurable.

### 3. Internationalization PT/EN (story 38)
Introduce a lightweight in-app i18n layer (no server). Header toggle switches PT⇄EN instantly without reload; all nav, titles, descriptions, actions, labels, and `aria-label`s are translated.
**Done when:** every user-facing string resolves through the i18n layer, toggling updates the whole UI live, and a test asserts a switch changes rendered copy.

### 4. Orders list → detail with breadcrumb (story 39)
Add a dedicated Orders list route (rows clickable + keyboard-accessible) that drills into the order detail, with a breadcrumb (list ⇄ detail) replacing the single back-link. The overview may keep its "recent orders" preview.
**Done when:** `/orders` lists orders, selecting one routes to `/orders/:id`, the breadcrumb returns to the list, and keyboard navigation works.

### 5. Request map view (story 40)
Add a request/event map: region view with routed connections (dotted), per-region volume, a live indicator, total requests + average latency, and a legend. Data is derived/representative where the backend does not expose geo/telemetry (per revised Out-of-Scope).
**Done when:** the map renders on the Overview (and/or its own route) with regions, routes, totals, latency, and legend, and is responsive + accessible.

### 6. Map node selection + detail panel (story 41)
Make each map node selectable (click and via a region dropdown); highlight the selection and update a detail panel with method, endpoint, origin, destination, status, latency, timestamp, and correlation id.
**Done when:** selecting a node (mouse or dropdown) updates the panel, the selected node is visually highlighted, and it is keyboard-operable.

### 7. AI analysis field on order detail (story 42)
Add an "AI analysis" panel to the order detail: automatic summary of the order's events, a confidence indicator + visual confidence bar, read-only, translated (PT/EN), accessible (`role`/`aria-readonly`). Backend has no ML service → summary is derived/representative client-side (per revised Out-of-Scope).
**Done when:** the panel renders on `/orders/:id` with a summary, confidence value + bar, is read-only, and localized.

### 8. JSON payload viewer on order detail (story 43)
Show the raw event payload as formatted JSON on the order detail so operators can inspect the exact pipeline message.
**Done when:** the order detail exposes a formatted, copyable JSON block of the event payload, styled in the mono type scale.

### 9. Throughput/lag + event-flow metrics (story 44)
Extend the metric surface beyond aggregate counts: throughput, consumer lag, and an event-flow view; each MetricCard carries a value, delta, and trend sparkline. `MetricCard` already supports `delta`/`trend`/`Sparkline` — wire them in the Overview and add the new metrics.
**Done when:** the Overview shows throughput and lag MetricCards with delta + sparkline, and an event-flow visualization is present.
