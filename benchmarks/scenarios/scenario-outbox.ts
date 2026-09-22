import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';

export async function runScenarioOutbox(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' OUTBOX BENCHMARK (Growth, Drain Rate & Backlog Convergence)');
  console.log('===============================================================');

  const db = ctx.getDb();
  await db.truncateBenchmarkData();
  await ctx.resetChaos();

  const collector = new MetricsCollector(db);
  const loadGen = new LoadGenerator();

  console.log('>> Ingesting 500 burst orders to measure Outbox growth...');
  await loadGen.runConstantLoad({
    ratePerSec: 100,
    durationSeconds: 5,
    userIdPrefix: 'user_outbox_burst',
  });
  loadGen.close();

  const peakMetrics = await collector.getDatabaseMetrics();
  const peakBacklog = peakMetrics.outbox_pending + peakMetrics.outbox_processing;

  console.log(`>> Peak observed Outbox Backlog: ${peakBacklog} events`);
  console.log('>> Waiting for Outbox Relay to drain pending backlog to 0...');

  const drainResult = await collector.waitForOutboxDrain(30);
  const drainDurationSec = drainResult.timeSec;

  const finalMetrics = await collector.getDatabaseMetrics();
  const drainRate = drainDurationSec > 0 ? parseFloat((finalMetrics.outbox_published_total / drainDurationSec).toFixed(2)) : 0;

  const result = {
    scenario: 'scenario-outbox-benchmark',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    ordersCreated: finalMetrics.orders_created_total,
    outboxEventsGenerated: finalMetrics.orders_created_total,
    outboxEventsPublished: finalMetrics.outbox_published_total,
    outboxEventsFailed: finalMetrics.outbox_failed_total,
    peakBacklog,
    finalBacklog: finalMetrics.outbox_pending,
    drainTimeSeconds: drainDurationSec,
    drainRateEventsPerSec: drainRate,
    backlogConvergedToZero: finalMetrics.outbox_pending === 0,
    lostPublications: Math.max(0, finalMetrics.orders_created_total - finalMetrics.outbox_published_total),
  };

  ctx.saveBenchmarkResult('outbox-benchmark', result);

  console.log('\n>> OUTBOX BENCHMARK RESULTS:');
  console.log(`   Orders Ingested: ${result.ordersCreated}`);
  console.log(`   Outbox Events Published: ${result.outboxEventsPublished}`);
  console.log(`   Peak Backlog Size: ${result.peakBacklog}`);
  console.log(`   Time to Drain: ${result.drainTimeSeconds}s`);
  console.log(`   Relay Drain Rate: ${result.drainRateEventsPerSec} events/sec`);
  console.log(`   Backlog Converged to 0: ${result.backlogConvergedToZero}`);
  console.log(`   Lost Publications: ${result.lostPublications}`);

  return result;
}
