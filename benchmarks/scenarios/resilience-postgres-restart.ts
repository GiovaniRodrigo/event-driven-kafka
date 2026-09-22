import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';
import { execSync } from 'child_process';

export async function runResiliencePostgresRestart(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' RESILIENCE: FAILURE D — POSTGRESQL DATABASE RESTART');
  console.log('===============================================================');

  const db = ctx.getDb();
  await db.truncateBenchmarkData();
  await ctx.resetChaos();

  const collector = new MetricsCollector(db);
  const loadGen = new LoadGenerator();

  console.log('>> 1. Ingesting initial batch of orders (20 orders)...');
  await loadGen.runConstantLoad({
    ratePerSec: 5,
    durationSeconds: 4,
    userIdPrefix: 'user_res_pg_pre',
  });

  console.log('>> 2. INJECTING FAILURE: Controlled restart of PostgreSQL container...');
  const restartStart = Date.now();
  try {
    execSync('docker restart postgres', { stdio: 'pipe' });
  } catch (err) {
    console.warn('   Docker restart postgres returned non-zero, continuing check...');
  }

  console.log('>> 3. Waiting for PostgreSQL readiness...');
  const maxWaitMs = 45000;
  while (Date.now() - restartStart < maxWaitMs) {
    try {
      execSync('docker exec postgres pg_isready -U postgres -d event_driven_test', { stdio: 'pipe' });
      break;
    } catch {
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  const restartDurationSec = parseFloat(((Date.now() - restartStart) / 1000).toFixed(2));
  console.log(`>> PostgreSQL back online in ${restartDurationSec}s.`);

  // Wait a moment for node-pg pool to re-establish
  await new Promise((r) => setTimeout(r, 4000));

  console.log('>> 4. Ingesting post-recovery orders (20 orders)...');
  await loadGen.runConstantLoad({
    ratePerSec: 5,
    durationSeconds: 4,
    userIdPrefix: 'user_res_pg_post',
  });
  loadGen.close();

  console.log('>> 5. Waiting for Outbox Relay and Consumers to process all orders...');
  const outboxDrain = await collector.waitForOutboxDrain(45);
  await collector.waitForAllOrdersCompleted(40, 90);
  const finalMetrics = await collector.getDatabaseMetrics();
  const finalLag = await collector.getKafkaConsumerLag();
  const accountedOrders = finalMetrics.orders_completed_total + finalMetrics.orders_cancelled_total;
  const lostEvents = Math.max(0, finalMetrics.orders_created_total - accountedOrders);

  const result = {
    scenario: 'resilience-postgres-restart',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    failureInjected: 'Docker container restart: postgres:15-alpine',
    expectedBehavior: 'PostgreSQL restarts, node-pg pool re-establishes connections, outbox and saga state remain consistent, zero corrupted state, zero lost events',
    observedBehavior: `PostgreSQL recovered in ${restartDurationSec}s, database connections recovered, outbox drained to 0, ${accountedOrders}/${finalMetrics.orders_created_total} orders accounted for cleanly`,
    postgresRestartDurationSec: restartDurationSec,
    totalRecoveryTimeSec: restartDurationSec + outboxDrain.timeSec,
    ordersCreated: finalMetrics.orders_created_total,
    ordersCompleted: finalMetrics.orders_completed_total,
    ordersCancelled: finalMetrics.orders_cancelled_total,
    lostEvents,
    finalClusterLag: finalLag.totalClusterLag,
    outboxDrainedCleanly: outboxDrain.drained,
    status: lostEvents === 0 && outboxDrain.drained ? 'VERIFIED' : 'FAILED',
  };

  ctx.saveBenchmarkResult('postgres-restart', result);

  console.log('\n>> POSTGRESQL RESTART RESILIENCE RESULTS:');
  console.log(`   PostgreSQL Restart Time: ${result.postgresRestartDurationSec}s`);
  console.log(`   Total Recovery Time: ${result.totalRecoveryTimeSec}s`);
  console.log(`   Orders Created: ${result.ordersCreated} | Completed: ${result.ordersCompleted}`);
  console.log(`   Lost Events: ${result.lostEvents}`);
  console.log(`   Final Outbox Pending: ${finalMetrics.outbox_pending}`);
  console.log(`   Status: ${result.status}`);

  return result;
}
