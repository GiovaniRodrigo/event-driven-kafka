import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';

export async function runScenarioABaseline(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' SCENARIO A: BASELINE BENCHMARK (10 req/s Sustained)');
  console.log('===============================================================');

  const db = ctx.getDb();
  await db.truncateBenchmarkData();
  await ctx.resetChaos();

  const collector = new MetricsCollector(db);
  const loadGen = new LoadGenerator();
  const startTime = new Date();

  console.log('>> Executing baseline load: 10 req/s for 30 seconds...');
  const loadResult = await loadGen.runConstantLoad({
    ratePerSec: 10,
    durationSeconds: 30,
    userIdPrefix: 'user_baseline',
  });
  loadGen.close();

  console.log('>> Waiting for Outbox Relay and Saga pipeline to settle...');
  const outboxDrain = await collector.waitForOutboxDrain(45);
  await collector.waitForAllOrdersCompleted(loadResult.successfulRequests, 60);
  const lagDrain = await collector.waitForConsumerLagZero(30);

  const finalDbMetrics = await collector.getDatabaseMetrics();
  const e2eStats = await collector.calculateE2ELatencies(startTime);
  const sagaStats = await collector.calculateSagaLatencies(startTime);
  const finalLagReport = await collector.getKafkaConsumerLag();

  const result = {
    scenario: 'scenario-a-baseline',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    workload: {
      targetRatePerSec: 10,
      durationSeconds: 30,
      totalOrdersTarget: 300,
    },
    httpMetrics: {
      totalRequests: loadResult.totalRequests,
      successfulRequests: loadResult.successfulRequests,
      failedRequests: loadResult.failedRequests,
      errorRatePercent: loadResult.errorRatePercent,
      actualThroughputReqPerSec: loadResult.actualThroughput,
      durationSeconds: loadResult.actualDurationSeconds,
      latency: loadResult.latencyStats,
    },
    outboxMetrics: {
      outboxPending: finalDbMetrics.outbox_pending,
      outboxPublishedTotal: finalDbMetrics.outbox_published_total,
      outboxFailedTotal: finalDbMetrics.outbox_failed_total,
      drainedCleanly: outboxDrain.drained,
      drainTimeSeconds: outboxDrain.timeSec,
    },
    kafkaMetrics: {
      finalTotalLag: finalLagReport.totalClusterLag,
      groupLags: finalLagReport.groups.map((g) => ({ group: g.consumerGroup, lag: g.totalLag })),
      lagRecoveryTimeSec: lagDrain.timeSec,
    },
    e2eLatency: e2eStats,
    sagaMetrics: sagaStats,
    dataIntegrity: {
      ordersCreated: finalDbMetrics.orders_created_total,
      ordersCompleted: finalDbMetrics.orders_completed_total,
      ordersCancelled: finalDbMetrics.orders_cancelled_total,
      eventStoreTotalEvents: finalDbMetrics.event_store_events_total,
      lostEvents: finalDbMetrics.orders_created_total - finalDbMetrics.orders_completed_total,
    },
  };

  ctx.saveBenchmarkResult('baseline', result);

  console.log('\n>> SCENARIO A RESULTS:');
  console.log(`   HTTP Ingestion Throughput: ${result.httpMetrics.actualThroughputReqPerSec} req/s`);
  console.log(`   HTTP Latency: p50=${result.httpMetrics.latency.p50}ms | p95=${result.httpMetrics.latency.p95}ms | p99=${result.httpMetrics.latency.p99}ms`);
  console.log(`   HTTP Error Rate: ${result.httpMetrics.errorRatePercent}%`);
  console.log(`   E2E Fulfillment Latency: p50=${result.e2eLatency.fulfillmentLatency.p50}ms | p95=${result.e2eLatency.fulfillmentLatency.p95}ms | p99=${result.e2eLatency.fulfillmentLatency.p99}ms | avg=${result.e2eLatency.fulfillmentLatency.avg}ms`);
  console.log(`   Saga Duration: p50=${result.sagaMetrics.successfulSagas.stats.p50}ms | p95=${result.sagaMetrics.successfulSagas.stats.p95}ms | p99=${result.sagaMetrics.successfulSagas.stats.p99}ms`);
  console.log(`   Orders Created: ${result.dataIntegrity.ordersCreated} | Completed: ${result.dataIntegrity.ordersCompleted} | Lost: ${result.dataIntegrity.lostEvents}`);
  console.log(`   Outbox Drained: ${result.outboxMetrics.drainedCleanly} in ${result.outboxMetrics.drainTimeSeconds}s | Cluster Lag: ${result.kafkaMetrics.finalTotalLag}`);

  return result;
}
