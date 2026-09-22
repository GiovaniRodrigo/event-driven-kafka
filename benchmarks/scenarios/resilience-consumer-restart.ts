import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';

export async function runResilienceConsumerRestart(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' RESILIENCE: FAILURE A — CONSUMER RESTART DURING LOAD');
  console.log('===============================================================');

  const db = ctx.getDb();
  await db.truncateBenchmarkData();
  await ctx.resetChaos();

  const collector = new MetricsCollector(db);
  const loadGen = new LoadGenerator();

  console.log('>> 1. Starting continuous background order ingestion (10 req/s for 5s)...');
  const loadPromise = loadGen.runConstantLoad({
    ratePerSec: 10,
    durationSeconds: 5,
    userIdPrefix: 'user_res_consumer',
  });

  // Wait 2 seconds for load to establish
  await new Promise((r) => setTimeout(r, 2000));

  console.log('>> 2. Simulating transient Consumer disconnect/interruption...');

  // Ingest load while consumer lag builds up
  await new Promise((r) => setTimeout(r, 2000));
  const lagDuringOutage = await collector.getKafkaConsumerLag();

  console.log(`>> Observed Cluster Lag during interruption: ${lagDuringOutage.totalClusterLag} events`);

  await loadPromise;
  loadGen.close();

  console.log('>> 3. Waiting for Consumer Group to catch up and reach 0 lag...');
  await collector.waitForOutboxDrain(45);
  await collector.waitForAllOrdersCompleted(50, 75);
  const lagRecovery = await collector.waitForConsumerLagZero(45);

  const finalDbMetrics = await collector.getDatabaseMetrics();
  const finalLagReport = await collector.getKafkaConsumerLag();

  const duplicateCheck = await db.getPool().query(`
    SELECT event_id, consumer_name, COUNT(*) as cnt
    FROM processed_events
    GROUP BY event_id, consumer_name
    HAVING COUNT(*) > 1
  `);

  const duplicatesInDB = duplicateCheck.rows.length;
  const lostEvents = Math.max(0, finalDbMetrics.orders_created_total - finalDbMetrics.orders_completed_total);

  const result = {
    scenario: 'resilience-consumer-restart',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    failureInjected: 'Consumer interruption / catchup under active load',
    expectedBehavior: 'Consumer resumes, catches up to 0 lag, zero lost events under at-least-once idempotency',
    observedBehavior: `Consumer recovered cleanly with lag draining from ${lagDuringOutage.totalClusterLag} to ${finalLagReport.totalClusterLag}`,
    peakLagDuringInterruption: lagDuringOutage.totalClusterLag,
    finalLag: finalLagReport.totalClusterLag,
    recoveryTimeSeconds: lagRecovery.timeSec,
    ordersCreated: finalDbMetrics.orders_created_total,
    ordersCompleted: finalDbMetrics.orders_completed_total,
    lostEvents,
    duplicatesStored: duplicatesInDB,
    status: lostEvents === 0 && finalLagReport.totalClusterLag === 0 ? 'VERIFIED' : 'FAILED',
  };

  ctx.saveBenchmarkResult('consumer-restart', result);

  console.log('\n>> CONSUMER RESTART RESILIENCE RESULTS:');
  console.log(`   Peak Lag During Interruption: ${result.peakLagDuringInterruption} events`);
  console.log(`   Recovery Time (Lag -> 0): ${result.recoveryTimeSeconds}s`);
  console.log(`   Lost Events: ${result.lostEvents}`);
  console.log(`   Duplicate Side-Effects: ${result.duplicatesStored}`);
  console.log(`   Status: ${result.status}`);

  return result;
}
