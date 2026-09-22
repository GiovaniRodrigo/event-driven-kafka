import http from 'http';
import { BenchmarkContext } from '../lib/benchmark-context';
import { LoadGenerator } from '../lib/load-generator';
import { MetricsCollector } from '../lib/metrics-collector';

export async function runScenarioRetryDLQ(ctx: BenchmarkContext): Promise<Record<string, any>> {
  console.log('\n===============================================================');
  console.log(' RETRY & DLQ BENCHMARK (Fault Injection, Backoff & Replay)');
  console.log('===============================================================');

  const db = ctx.getDb();
  await db.truncateBenchmarkData();
  await ctx.resetChaos();

  const collector = new MetricsCollector(db);
  const loadGen = new LoadGenerator();

  console.log('>> 1. Injecting Chaos Fault: Notification Service Failure (triggers exception & DLQ)...');
  await ctx.setChaosFault('/chaos/notification/down', { enabled: true });

  console.log('>> 2. Ingesting 10 orders destined for notification failure & DLQ...');
  await loadGen.runConstantLoad({
    ratePerSec: 5,
    durationSeconds: 2,
    userIdPrefix: 'user_dlq_test',
  });
  loadGen.close();

  console.log('>> 3. Waiting for retry exhaustion (3 attempts with exponential backoff) & DLQ routing...');
  // Max retries = 3, baseBackoff = 500ms, retry 1 ~500ms, retry 2 ~1000ms, total ~3-5s
  await new Promise((r) => setTimeout(r, 8000));
  await collector.waitForOutboxDrain(15);

  const dlqMessagesRes = await db.listDLQMessages(50, 'UNRESOLVED');
  const unresolvedDLQCount = dlqMessagesRes.length;

  console.log(`>> Observed Unresolved DLQ Messages: ${unresolvedDLQCount}`);

  console.log('>> 4. Resetting Chaos Faults to restore downstream health...');
  await ctx.resetChaos();

  console.log('>> 5. Replaying DLQ messages via Replay Service API...');
  let successfulReplays = 0;
  let failedReplays = 0;

  for (const msg of dlqMessagesRes) {
    try {
      const replayRes = await replayDlqMessage(msg.id);
      if (replayRes && replayRes.status === 'SUCCESS') {
        successfulReplays++;
      } else {
        failedReplays++;
      }
    } catch {
      failedReplays++;
    }
  }

  await new Promise((r) => setTimeout(r, 4000));
  await collector.waitForOutboxDrain(15);
  await collector.waitForConsumerLagZero(15);

  const remainingUnresolved = (await db.listDLQMessages(50, 'UNRESOLVED')).length;
  const replayedCount = (await db.listDLQMessages(50, 'REPLAYED')).length;

  const result = {
    scenario: 'scenario-retry-dlq',
    timestamp: new Date().toISOString(),
    environment: ctx.getEnvironmentInfo(),
    faultInjected: 'notification_failure',
    retryConfiguration: {
      maxRetries: 3,
      baseBackoffMs: 500,
      backoffStrategy: 'Exponential with random jitter',
    },
    dlqMetrics: {
      messagesGenerated: unresolvedDLQCount,
      replayedSuccessfully: successfulReplays,
      replayFailures: failedReplays,
      finalUnresolvedCount: remainingUnresolved,
      finalReplayedCount: replayedCount,
      dlqPersistenceVerified: unresolvedDLQCount > 0,
      dlqReplayVerified: successfulReplays > 0 && remainingUnresolved === 0,
    },
  };

  ctx.saveBenchmarkResult('retry-dlq', result);

  console.log('\n>> RETRY / DLQ RESULTS:');
  console.log(`   DLQ Messages Generated on Failure: ${result.dlqMetrics.messagesGenerated}`);
  console.log(`   DLQ Messages Successfully Replayed: ${result.dlqMetrics.replayedSuccessfully}`);
  console.log(`   Remaining Unresolved in DLQ: ${result.dlqMetrics.finalUnresolvedCount}`);
  console.log(`   DLQ Persistence Verified: ${result.dlqMetrics.dlqPersistenceVerified}`);
  console.log(`   DLQ Replay Flow Verified: ${result.dlqMetrics.dlqReplayVerified}`);

  return result;
}

function replayDlqMessage(dlqId: string): Promise<any> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: 'localhost',
        port: 3000,
        path: `/dlq/${dlqId}/replay`,
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        timeout: 5000,
      },
      (res) => {
        let body = '';
        res.on('data', (c) => (body += c));
        res.on('end', () => {
          try {
            resolve(JSON.parse(body));
          } catch {
            resolve({ raw: body });
          }
        });
      }
    );
    req.on('error', (e) => reject(e));
    req.end();
  });
}
