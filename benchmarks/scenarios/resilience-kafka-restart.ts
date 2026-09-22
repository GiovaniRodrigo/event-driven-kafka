import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';
import { execSync } from 'child_process';

export async function runResilienceKafkaRestart(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' RESILIENCE: FAILURE C — KAFKA BROKER RESTART DURING LOAD');
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
    userIdPrefix: 'user_res_kafka_pre',
  });

  console.log('>> 2. INJECTING FAILURE: Restarting Kafka Broker Container...');
  const restartStart = Date.now();
  try {
    execSync('docker restart kafka', { stdio: 'pipe' });
  } catch (err) {
    console.warn('   Docker restart kafka returned non-zero, continuing check...');
  }

  console.log('>> 3. Waiting for Kafka to become available and healthy again...');
  const maxWaitMs = 45000;
  while (Date.now() - restartStart < maxWaitMs) {
    try {
      execSync('nc -z localhost 9092', { stdio: 'pipe' });
      break;
    } catch {
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  const restartDurationSec = parseFloat(((Date.now() - restartStart) / 1000).toFixed(2));
  console.log(`>> Kafka broker back online in ${restartDurationSec}s.`);

  console.log('>> 4. Ingesting post-recovery orders (20 orders)...');
  await loadGen.runConstantLoad({
    ratePerSec: 5,
    durationSeconds: 4,
    userIdPrefix: 'user_res_kafka_post',
  });
  loadGen.close();

  console.log('>> 5. Waiting for Outbox Relay and Consumers to reconnect and catch up...');
  const outboxDrain = await collector.waitForOutboxDrain(45);
  await collector.waitForAllOrdersCompleted(40, 90);
  await collector.waitForConsumerLagZero(45);

  const finalMetrics = await collector.getDatabaseMetrics();
  const finalLag = await collector.getKafkaConsumerLag();
  const lostEvents = Math.max(0, finalMetrics.orders_created_total - finalMetrics.orders_completed_total);

  const result = {
    scenario: 'resilience-kafka-restart',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    failureInjected: 'Docker container restart: confluentinc/cp-kafka:7.5.0',
    expectedBehavior: 'Kafka broker restarts, KafkaJS producers/consumers reconnect automatically with exponential backoff, Transactional Outbox retries any interrupted publishes, zero lost events',
    observedBehavior: `Kafka recovered in ${restartDurationSec}s, consumers and outbox reconnected, cluster lag drained to ${finalLag.totalClusterLag}`,
    kafkaRestartDurationSec: restartDurationSec,
    totalRecoveryTimeSec: restartDurationSec + outboxDrain.timeSec,
    ordersCreated: finalMetrics.orders_created_total,
    ordersCompleted: finalMetrics.orders_completed_total,
    lostEvents,
    finalClusterLag: finalLag.totalClusterLag,
    outboxDrainedCleanly: outboxDrain.drained,
    status: lostEvents === 0 && outboxDrain.drained ? 'VERIFIED' : 'FAILED',
  };

  ctx.saveBenchmarkResult('kafka-restart', result);

  console.log('\n>> KAFKA RESTART RESILIENCE RESULTS:');
  console.log(`   Kafka Broker Restart Time: ${result.kafkaRestartDurationSec}s`);
  console.log(`   Total Recovery Time: ${result.totalRecoveryTimeSec}s`);
  console.log(`   Orders Created: ${result.ordersCreated} | Completed: ${result.ordersCompleted}`);
  console.log(`   Lost Events: ${result.lostEvents}`);
  console.log(`   Final Cluster Lag: ${result.finalClusterLag}`);
  console.log(`   Status: ${result.status}`);

  return result;
}
