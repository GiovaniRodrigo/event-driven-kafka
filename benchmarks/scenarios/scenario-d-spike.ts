import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';

export async function runScenarioDSpike(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' SCENARIO D: SPIKE LOAD BENCHMARK (10 req/s -> 150 req/s -> 10 req/s)');
  console.log('===============================================================');

  const db = ctx.getDb();
  await db.truncateBenchmarkData();
  await ctx.resetChaos();

  const collector = new MetricsCollector(db);
  const loadGen = new LoadGenerator();
  const startTime = new Date();

  console.log('>> 1. Running Pre-Spike Baseline (10 req/s for 10s)...');
  const preSpike = await loadGen.runConstantLoad({
    ratePerSec: 10,
    durationSeconds: 10,
    userIdPrefix: 'user_prespike',
  });

  const preSpikeLag = await collector.getKafkaConsumerLag();

  console.log('>> 2. INJECTING SUDDEN TRAFFIC SPIKE (150 req/s for 10s)...');
  const spike = await loadGen.runConstantLoad({
    ratePerSec: 150,
    durationSeconds: 10,
    userIdPrefix: 'user_spike',
  });

  const peakLagReport = await collector.getKafkaConsumerLag();
  const peakDbMetrics = await collector.getDatabaseMetrics();

  console.log('>> 3. Returning to Normal Baseline Traffic (10 req/s for 15s)...');
  const postSpike = await loadGen.runConstantLoad({
    ratePerSec: 10,
    durationSeconds: 15,
    userIdPrefix: 'user_postspike',
  });
  loadGen.close();

  console.log('>> 4. Measuring Outbox drain and Kafka lag convergence to 0...');
  const recoveryStart = Date.now();
  const outboxDrain = await collector.waitForOutboxDrain(60);
  const totalTargetOrders = preSpike.successfulRequests + spike.successfulRequests + postSpike.successfulRequests;
  await collector.waitForAllOrdersCompleted(totalTargetOrders, 90);
  const lagDrain = await collector.waitForConsumerLagZero(45);
  const recoveryTimeSec = parseFloat(((Date.now() - recoveryStart) / 1000).toFixed(2));

  const finalDbMetrics = await collector.getDatabaseMetrics();
  const e2eStats = await collector.calculateE2ELatencies(startTime);
  const sagaStats = await collector.calculateSagaLatencies(startTime);
  const finalLagReport = await collector.getKafkaConsumerLag();

  const totalRequests = preSpike.totalRequests + spike.totalRequests + postSpike.totalRequests;
  const successfulRequests = preSpike.successfulRequests + spike.successfulRequests + postSpike.successfulRequests;
  const failedRequests = preSpike.failedRequests + spike.failedRequests + postSpike.failedRequests;
  const overallErrorRate = totalRequests > 0 ? parseFloat(((failedRequests / totalRequests) * 100).toFixed(2)) : 0;

  const result = {
    scenario: 'scenario-d-spike-load',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    phases: {
      preSpike: {
        targetRateReqPerSec: 10,
        actualThroughputReqPerSec: preSpike.actualThroughput,
        requests: preSpike.totalRequests,
        latency: preSpike.latencyStats,
        clusterLag: preSpikeLag.totalClusterLag,
      },
      spike: {
        targetRateReqPerSec: 150,
        actualThroughputReqPerSec: spike.actualThroughput,
        requests: spike.totalRequests,
        latency: spike.latencyStats,
        observedPeakClusterLag: peakLagReport.totalClusterLag,
        observedPeakOutboxBacklog: peakDbMetrics.outbox_pending + peakDbMetrics.outbox_processing,
      },
      postSpike: {
        targetRateReqPerSec: 10,
        actualThroughputReqPerSec: postSpike.actualThroughput,
        requests: postSpike.totalRequests,
        latency: postSpike.latencyStats,
      },
    },
    recoveryMetrics: {
      peakClusterLag: peakLagReport.totalClusterLag,
      peakOutboxPending: peakDbMetrics.outbox_pending,
      finalClusterLag: finalLagReport.totalClusterLag,
      finalOutboxPending: finalDbMetrics.outbox_pending,
      lagRecoveryTimeSec: lagDrain.timeSec,
      outboxDrainTimeSec: outboxDrain.timeSec,
      totalRecoveryTimeSec: recoveryTimeSec,
      outboxDrainedCleanly: outboxDrain.drained,
      lagRecoveredToZero: lagDrain.zeroLag,
    },
    summary: {
      totalRequests,
      successfulRequests,
      failedRequests,
      overallErrorRatePercent: overallErrorRate,
      peakLatencyMs: Math.max(preSpike.latencyStats.max, spike.latencyStats.max, postSpike.latencyStats.max),
    },
    outboxMetrics: {
      outboxPending: finalDbMetrics.outbox_pending,
      outboxPublishedTotal: finalDbMetrics.outbox_published_total,
      outboxFailedTotal: finalDbMetrics.outbox_failed_total,
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

  ctx.saveBenchmarkResult('spike-load', result);

  console.log('\n>> SCENARIO D RESULTS:');
  console.log(`   Peak Kafka Cluster Lag: ${result.recoveryMetrics.peakClusterLag} events`);
  console.log(`   Peak Outbox Backlog: ${result.recoveryMetrics.peakOutboxPending} events`);
  console.log(`   Time to Recover Lag -> 0: ${result.recoveryMetrics.lagRecoveryTimeSec}s`);
  console.log(`   Time to Drain Outbox -> 0: ${result.recoveryMetrics.outboxDrainTimeSec}s`);
  console.log(`   Spike Phase Latency: p50=${spike.latencyStats.p50}ms | p95=${spike.latencyStats.p95}ms | p99=${spike.latencyStats.p99}ms | max=${spike.latencyStats.max}ms`);
  console.log(`   Orders Created: ${result.dataIntegrity.ordersCreated} | Completed: ${result.dataIntegrity.ordersCompleted} | Lost: ${result.dataIntegrity.lostEvents}`);

  return result;
}
