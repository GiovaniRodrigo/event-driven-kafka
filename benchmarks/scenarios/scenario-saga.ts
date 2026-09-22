import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';

export async function runScenarioSaga(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' SAGA BENCHMARK (Successful vs Compensating Saga Latencies)');
  console.log('===============================================================');

  const db = ctx.getDb();
  await db.truncateBenchmarkData();
  await ctx.resetChaos();

  const collector = new MetricsCollector(db);
  const loadGen = new LoadGenerator();

  console.log('>> 1. Benchmarking SUCCESSFUL Saga Executions (100 orders)...');
  const successStart = new Date();
  await loadGen.runConstantLoad({
    ratePerSec: 20,
    durationSeconds: 5,
    userIdPrefix: 'user_saga_success',
  });

  await collector.waitForOutboxDrain(30);
  await collector.waitForAllOrdersCompleted(100, 60);
  await collector.waitForConsumerLagZero(30);

  const successSagaStats = await collector.calculateSagaLatencies(successStart);

  console.log('>> 2. Injecting Chaos: Inventory Reservation Failure to trigger Compensations...');
  await ctx.setChaosFault('/chaos/inventory/failure', { enabled: true });

  const compStart = new Date();
  await loadGen.runConstantLoad({
    ratePerSec: 20,
    durationSeconds: 5,
    userIdPrefix: 'user_saga_comp',
  });
  loadGen.close();

  await collector.waitForOutboxDrain(30);
  await collector.waitForAllOrdersCompleted(200, 60);
  await collector.waitForConsumerLagZero(30);

  const compSagaStats = await collector.calculateSagaLatencies(compStart);
  await ctx.resetChaos();

  const finalDbMetrics = await collector.getDatabaseMetrics();

  const result = {
    scenario: 'scenario-saga-latency',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    successfulSagas: {
      count: successSagaStats.successfulSagas.count,
      latency: successSagaStats.successfulSagas.stats,
    },
    compensatingSagas: {
      count: compSagaStats.compensatingSagas.count,
      latency: compSagaStats.compensatingSagas.stats,
    },
    failedSagas: compSagaStats.failedSagas,
    summary: {
      totalSagasObserved: finalDbMetrics.saga_started_total,
      completedSagas: finalDbMetrics.saga_completed_total,
      compensatedSagas: finalDbMetrics.saga_compensated_total,
    },
  };

  ctx.saveBenchmarkResult('saga-latency', result);

  console.log('\n>> SAGA BENCHMARK RESULTS:');
  console.log(`   Successful Sagas: count=${result.successfulSagas.count} | p50=${result.successfulSagas.latency.p50}ms | p95=${result.successfulSagas.latency.p95}ms | p99=${result.successfulSagas.latency.p99}ms | avg=${result.successfulSagas.latency.avg}ms`);
  console.log(`   Compensating Sagas: count=${result.compensatingSagas.count} | p50=${result.compensatingSagas.latency.p50}ms | p95=${result.compensatingSagas.latency.p95}ms | p99=${result.compensatingSagas.latency.p99}ms | avg=${result.compensatingSagas.latency.avg}ms`);
  console.log(`   Total Sagas: ${result.summary.totalSagasObserved} (Completed: ${result.summary.completedSagas}, Compensated: ${result.summary.compensatedSagas})`);

  return result;
}
