import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';

export async function runScenarioE2ELatency(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' END-TO-END FULFILLMENT LATENCY BENCHMARK (Correlation-Based)');
  console.log('===============================================================');

  const db = ctx.getDb();
  await db.truncateBenchmarkData();
  await ctx.resetChaos();

  const collector = new MetricsCollector(db);
  const loadGen = new LoadGenerator();
  const startTime = new Date();

  console.log('>> Ingesting 200 orders to calculate correlation-based stage latencies...');
  const loadResult = await loadGen.runConstantLoad({
    ratePerSec: 20,
    durationSeconds: 10,
    userIdPrefix: 'user_e2e_lat',
  });
  loadGen.close();

  console.log('>> Waiting for all orders to complete fulfillment...');
  await collector.waitForOutboxDrain(45);
  await collector.waitForAllOrdersCompleted(loadResult.successfulRequests, 90);
  await collector.waitForConsumerLagZero(30);

  const e2eReport = await collector.calculateE2ELatencies(startTime);

  const result = {
    scenario: 'scenario-e2e-latency',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    httpIngestionLatency: loadResult.latencyStats,
    endToEndFulfillmentLatency: e2eReport.fulfillmentLatency,
    stepLatencies: e2eReport.stepLatencies,
    summary: {
      totalOrdersSubmitted: loadResult.totalRequests,
      totalOrdersCompleted: e2eReport.totalCompletedOrders,
      fulfillmentSuccessRatePercent:
        loadResult.totalRequests > 0
          ? parseFloat(((e2eReport.totalCompletedOrders / loadResult.totalRequests) * 100).toFixed(2))
          : 0,
    },
  };

  ctx.saveBenchmarkResult('e2e-latency', result);

  console.log('\n>> END-TO-END LATENCY BREAKDOWN:');
  console.log(`   HTTP POST /orders (Ingestion): p50=${loadResult.latencyStats.p50}ms | p95=${loadResult.latencyStats.p95}ms | p99=${loadResult.latencyStats.p99}ms | avg=${loadResult.latencyStats.avg}ms`);
  console.log(`   Total E2E Pipeline (OrderCreated -> OrderCompleted):`);
  console.log(`     p50: ${e2eReport.fulfillmentLatency.p50}ms`);
  console.log(`     p95: ${e2eReport.fulfillmentLatency.p95}ms`);
  console.log(`     p99: ${e2eReport.fulfillmentLatency.p99}ms`);
  console.log(`     min: ${e2eReport.fulfillmentLatency.min}ms | max: ${e2eReport.fulfillmentLatency.max}ms | avg: ${e2eReport.fulfillmentLatency.avg}ms`);
  console.log('   Step Breakdown:');
  for (const step of e2eReport.stepLatencies) {
    console.log(`     - ${step.step.padEnd(42)}: p50=${step.p50.toString().padStart(4)}ms | p95=${step.p95.toString().padStart(4)}ms | avg=${step.avg}ms`);
  }

  return result;
}
