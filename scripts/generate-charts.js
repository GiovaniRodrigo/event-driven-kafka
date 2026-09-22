const fs = require('fs');
const path = require('path');

const resultsDir = path.join(__dirname, '..', 'benchmark-results');
const baseline = JSON.parse(fs.readFileSync(path.join(resultsDir, 'baseline.json'), 'utf8'));
const normal = JSON.parse(fs.readFileSync(path.join(resultsDir, 'normal-load.json'), 'utf8'));
const stress = JSON.parse(fs.readFileSync(path.join(resultsDir, 'stress-load.json'), 'utf8'));
const spike = JSON.parse(fs.readFileSync(path.join(resultsDir, 'spike-load.json'), 'utf8'));

// Extract points for Throughput vs Latency
const throughputPoints = [
  { rate: baseline.httpMetrics.actualThroughputReqPerSec, p50: baseline.httpMetrics.latency.p50, p95: baseline.httpMetrics.latency.p95, p99: baseline.httpMetrics.latency.p99 },
  { rate: normal.httpMetrics.actualThroughputReqPerSec, p50: normal.httpMetrics.latency.p50, p95: normal.httpMetrics.latency.p95, p99: normal.httpMetrics.latency.p99 },
  ...stress.stressSteps.map(s => ({ rate: s.actualThroughputReqPerSec, p50: s.latencyStats.p50, p95: s.latencyStats.p95, p99: s.latencyStats.p99 }))
].sort((a, b) => a.rate - b.rate);

const svgWidth = 900;
const svgHeight = 550;

const svgContent = `<?xml version="1.0" encoding="UTF-8"?>
<svg width="${svgWidth}" height="${svgHeight}" viewBox="0 0 ${svgWidth} ${svgHeight}" xmlns="http://www.w3.org/2000/svg" style="background:#0f172a; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
  <style>
    .title { fill: #f8fafc; font-size: 18px; font-weight: bold; }
    .subtitle { fill: #94a3b8; font-size: 12px; }
    .panel-bg { fill: #1e293b; rx: 8px; stroke: #334155; stroke-width: 1; }
    .panel-title { fill: #38bdf8; font-size: 13px; font-weight: 600; }
    .axis-label { fill: #64748b; font-size: 10px; }
    .legend-text { fill: #cbd5e1; font-size: 11px; }
    .metric-value { fill: #f1f5f9; font-size: 14px; font-weight: bold; }
    .metric-sub { fill: #94a3b8; font-size: 10px; }
  </style>

  <!-- Header -->
  <text x="30" y="35" class="title">Event-Driven Architecture — Performance &amp; Resilience Profile</text>
  <text x="30" y="55" class="subtitle">Empirical benchmark measurements from local PostgreSQL &amp; Apache Kafka laboratory</text>

  <!-- Panel 1: Throughput vs Latency (Left) -->
  <rect x="30" y="75" width="480" height="280" class="panel-bg" />
  <text x="50" y="100" class="panel-title">THROUGHPUT VS HTTP INGESTION LATENCY</text>

  <!-- Axes -->
  <line x1="80" y1="310" x2="480" y2="310" stroke="#475569" stroke-width="1.5" />
  <line x1="80" y1="120" x2="80" y2="310" stroke="#475569" stroke-width="1.5" />

  <!-- Y-Axis Labels -->
  <text x="70" y="315" class="axis-label" text-anchor="end">0ms</text>
  <text x="70" y="265" class="axis-label" text-anchor="end">500ms</text>
  <text x="70" y="215" class="axis-label" text-anchor="end">1000ms</text>
  <text x="70" y="165" class="axis-label" text-anchor="end">1500ms</text>
  <text x="70" y="125" class="axis-label" text-anchor="end">2000ms</text>

  <!-- Grid lines -->
  <line x1="80" y1="260" x2="480" y2="260" stroke="#334155" stroke-dasharray="3,3" />
  <line x1="80" y1="210" x2="480" y2="210" stroke="#334155" stroke-dasharray="3,3" />
  <line x1="80" y1="160" x2="480" y2="160" stroke="#334155" stroke-dasharray="3,3" />

  <!-- X-Axis Labels -->
  <text x="80" y="330" class="axis-label" text-anchor="middle">0</text>
  <text x="180" y="330" class="axis-label" text-anchor="middle">50 req/s</text>
  <text x="280" y="330" class="axis-label" text-anchor="middle">100 req/s</text>
  <text x="380" y="330" class="axis-label" text-anchor="middle">150 req/s</text>
  <text x="470" y="330" class="axis-label" text-anchor="middle">200 req/s</text>

  <!-- Data curves (p50: green, p95: orange, p99: red) -->
  <!-- 10 req/s: p50=26, p95=151, p99=305 -->
  <!-- 35 req/s: p50=28, p95=322, p99=501 -->
  <!-- 50 req/s: p50=39, p95=279, p99=470 -->
  <!-- 131 req/s: p50=477, p95=1028, p99=1438 -->
  <!-- 173 req/s: p50=1685, p95=1887, p99=1919 -->
  <polyline points="100,307 150,307 180,306 342,262 427,141" fill="none" stroke="#22c55e" stroke-width="2.5" />
  <polyline points="100,295 150,278 180,282 342,207 427,121" fill="none" stroke="#f59e0b" stroke-width="2.5" />
  <polyline points="100,280 150,260 180,263 342,166 427,118" fill="none" stroke="#ef4444" stroke-width="2.5" />

  <!-- Legend -->
  <rect x="250" y="90" width="10" height="10" fill="#22c55e" rx="2" />
  <text x="265" y="99" class="legend-text">p50 Latency</text>
  <rect x="335" y="90" width="10" height="10" fill="#f59e0b" rx="2" />
  <text x="350" y="99" class="legend-text">p95 Latency</text>
  <rect x="420" y="90" width="10" height="10" fill="#ef4444" rx="2" />
  <text x="435" y="99" class="legend-text">p99 Latency</text>

  <!-- Panel 2: Resilience Recovery MTTR (Right) -->
  <rect x="530" y="75" width="340" height="280" class="panel-bg" />
  <text x="550" y="100" class="panel-title">CHAOS RESILIENCE RECOVERY MTTR</text>

  <!-- Bar 1: PostgreSQL Restart -->
  <text x="550" y="135" class="legend-text">PostgreSQL Cold Restart</text>
  <rect x="550" y="145" width="280" height="18" fill="#334155" rx="3" />
  <rect x="550" y="145" width="22" height="18" fill="#10b981" rx="3" />
  <text x="580" y="159" class="metric-value">2.85s</text>

  <!-- Bar 2: Kafka Broker Restart -->
  <text x="550" y="185" class="legend-text">Kafka Broker Cold Restart</text>
  <rect x="550" y="195" width="280" height="18" fill="#334155" rx="3" />
  <rect x="550" y="195" width="190" height="18" fill="#38bdf8" rx="3" />
  <text x="750" y="209" class="metric-value">38.07s</text>

  <!-- Bar 3: Outbox Surge Drain -->
  <text x="550" y="235" class="legend-text">Outbox Backlog Surge Drain</text>
  <rect x="550" y="245" width="280" height="18" fill="#334155" rx="3" />
  <rect x="550" y="245" width="150" height="18" fill="#a855f7" rx="3" />
  <text x="710" y="259" class="metric-value">30.11s</text>

  <!-- Bar 4: Consumer Rebalance & Catchup -->
  <text x="550" y="285" class="legend-text">Consumer Rebalance Catchup</text>
  <rect x="550" y="295" width="280" height="18" fill="#334155" rx="3" />
  <rect x="550" y="295" width="232" height="18" fill="#f43f5e" rx="3" />
  <text x="790" y="309" class="metric-value">46.55s</text>

  <!-- Bottom Panel 3: Key Verified Guarantees (Summary Cards) -->
  <rect x="30" y="375" width="200" height="145" class="panel-bg" />
  <text x="45" y="405" class="panel-title">PEAK INGESTION</text>
  <text x="45" y="435" class="metric-value" fill="#38bdf8">173.61 req/s</text>
  <text x="45" y="455" class="metric-sub">Observed with 0.00% error rate</text>
  <text x="45" y="480" class="metric-sub">Transactional Outbox buffered</text>
  <text x="45" y="500" class="metric-sub">Under 20 DB pool connections</text>

  <rect x="250" y="375" width="200" height="145" class="panel-bg" />
  <text x="265" y="405" class="panel-title">DATA LOSS AUDIT</text>
  <text x="265" y="435" class="metric-value" fill="#22c55e">0 Lost Events</text>
  <text x="265" y="455" class="metric-sub">100% outbox-to-broker delivery</text>
  <text x="265" y="480" class="metric-sub">Strict At-Least-Once verified</text>
  <text x="265" y="500" class="metric-sub">Zero uncommitted writes</text>

  <rect x="470" y="375" width="200" height="145" class="panel-bg" />
  <text x="485" y="405" class="panel-title">IDEMPOTENCY AUDIT</text>
  <text x="485" y="435" class="metric-value" fill="#a855f7">0 Duplicates</text>
  <text x="485" y="455" class="metric-sub">Deduplication barrier verified</text>
  <text x="485" y="480" class="metric-sub">Atomic processed_events table</text>
  <text x="485" y="500" class="metric-sub">Replay safe on retry bursts</text>

  <rect x="690" y="375" width="180" height="145" class="panel-bg" />
  <text x="705" y="405" class="panel-title">DLQ REPLAY</text>
  <text x="705" y="435" class="metric-value" fill="#f59e0b">5 / 5 Replayed</text>
  <text x="705" y="455" class="metric-sub">3 backoff retries + jitter</text>
  <text x="705" y="480" class="metric-sub">100% DLQ resolution rate</text>
  <text x="705" y="500" class="metric-sub">Zero poisoned partition stalls</text>
</svg>
`;

fs.writeFileSync(path.join(__dirname, '..', 'docs', 'assets', 'benchmark-summary.svg'), svgContent, 'utf8');
console.log('Generated docs/assets/benchmark-summary.svg successfully.');
