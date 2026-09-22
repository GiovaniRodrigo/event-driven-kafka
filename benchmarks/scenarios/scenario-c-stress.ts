import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator, StepProfile } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';

export async function runScenarioCStress(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' SCENARIO C: STRESS LOAD BENCHMARK (Stepped Ramping Load)');
  console.log('===============================================================');

  const db = ctx.getDb();
  await db.truncateBenchmarkData();
  await ctx.resetChaos();

  const collector = new MetricsCollector(db);
  const loadGen = new LoadGenerator();
  const startTime = new Date();

  const stressSteps: StepProfile[] = [
    { targetRate: 25, durationSeconds: 10 },
    { targetRate: 50, durationSeconds: 10 },
    { targetRate: 100, durationSeconds: 10 },
    { targetRate: 150, durationSeconds: 10 },
    { targetRate: 200, durationSeconds: 10 },
    { targetRate: 250, durationSeconds: 10 },
  ];

  console.log('>> Executing stepped stress test (25 -> 50 -> 100 -> 150 -> 200 -> 250 req/s)...');
  const stepReports = [];
  let maxSustainableObservedThroughput = 0;
  let latencyDegradationPoint: number | null = null;
  let errorInflectionPoint: number | null = null;

  for (const step of stressSteps) {
    console.log(`   Step: Target ${step.targetRate} req/s for ${step.durationSeconds}s...`);
    const stepResult = await loadGen.runConstantLoad({
      ratePerSec: step.targetRate,
      durationSeconds: step.durationSeconds,
      userIdPrefix: `user_stress_${step.targetRate}`,
    });

    const lagDuringStep = await collector.getKafkaConsumerLag();

    const report = {
      targetRateReqPerSec: step.targetRate,
      actualThroughputReqPerSec: stepResult.actualThroughput,
      totalRequests: stepResult.totalRequests,
      successfulRequests: stepResult.successfulRequests,
      failedRequests: stepResult.failedRequests,
      errorRatePercent: stepResult.errorRatePercent,
      latencyStats: stepResult.latencyStats,
      clusterLagDuringStep: lagDuringStep.totalClusterLag,
    };

    stepReports.push(report);

    // Analyze thresholds
    if (stepResult.errorRatePercent <= 1.0 && stepResult.actualThroughput > maxSustainableObservedThroughput) {
      maxSustainableObservedThroughput = stepResult.actualThroughput;
    }

    if (latencyDegradationPoint === null && (stepResult.latencyStats.p95 > 100 || stepResult.latencyStats.p99 > 200)) {
      latencyDegradationPoint = step.targetRate;
    }

    if (errorInflectionPoint === null && stepResult.errorRatePercent > 0.5) {
      errorInflectionPoint = step.targetRate;
    }
  }

  loadGen.close();

  console.log('>> Waiting for Outbox Relay and Saga pipeline to settle after stress load...');
  const outboxDrain = await collector.waitForOutboxDrain(60);
  const totalSuccessTarget = stepReports.reduce((s, r) => s + r.successfulRequests, 0);
  await collector.waitForAllOrdersCompleted(totalSuccessTarget, 120);
  const lagDrain = await collector.waitForConsumerLagZero(45);

  const finalDbMetrics = await collector.getDatabaseMetrics();
  const e2eStats = await collector.calculateE2ELatencies(startTime);
  const sagaStats = await collector.calculateSagaLatencies(startTime);
  const finalLagReport = await collector.getKafkaConsumerLag();

  const totalRequests = stepReports.reduce((sum, s) => sum + s.totalRequests, 0);
  const successfulRequests = stepReports.reduce((sum, s) => sum + s.successfulRequests, 0);
  const failedRequests = stepReports.reduce((sum, s) => sum + s.failedRequests, 0);
  const overallErrorRate = totalRequests > 0 ? parseFloat(((failedRequests / totalRequests) * 100).toFixed(2)) : 0;

  const result = {
    scenario: 'scenario-c-stress-load',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    stressSteps: stepReports,
    summary: {
      totalRequests,
      successfulRequests,
      failedRequests,
      overallErrorRatePercent: overallErrorRate,
      maxSustainableObservedThroughputReqPerSec: maxSustainableObservedThroughput,
      latencyDegradationPointReqPerSec: latencyDegradationPoint || 'None observed within range',
      errorInflectionPointReqPerSec: errorInflectionPoint || 'None observed within range',
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

  ctx.saveBenchmarkResult('stress-load', result);

  console.log('\n>> SCENARIO C RESULTS:');
  console.log(`   Total Requests: ${totalRequests} | Successful: ${successfulRequests} | Failed: ${failedRequests}`);
  console.log(`   Max Sustainable Observed Throughput: ${maxSustainableObservedThroughput} req/s`);
  console.log(`   Latency Degradation Point: ${latencyDegradationPoint || 'None observed'}`);
  console.log(`   Error-rate Inflection Point: ${errorInflectionPoint || 'None observed'}`);
  console.log(`   E2E Fulfillment Latency: p50=${result.e2eLatency.fulfillmentLatency.p50}ms | p95=${result.e2eLatency.fulfillmentLatency.p95}ms | p99=${result.e2eLatency.fulfillmentLatency.p99}ms`);
  console.log(`   Orders Created: ${result.dataIntegrity.ordersCreated} | Completed: ${result.dataIntegrity.ordersCompleted} | Lost: ${result.dataIntegrity.lostEvents}`);
  console.log(`   Outbox Drained: ${result.outboxMetrics.drainedCleanly} in ${result.outboxMetrics.drainTimeSeconds}s | Cluster Lag: ${result.kafkaMetrics.finalTotalLag}`);

  return result;
}
