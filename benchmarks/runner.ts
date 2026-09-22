import http from 'http';
import fs from 'fs';
import path from 'path';
import { Server } from 'socket.io';
import { Kafka } from 'kafkajs';
import { createApp } from '../src/app';
import { DatabaseService } from '../src/services/database';
import { OutboxRelay } from '../src/infrastructure/outbox/outbox-relay';
import { OrderService } from '../src/services/order-service';
import { PaymentConsumer } from '../src/consumers/payment-consumer';
import { InventoryConsumer } from '../src/consumers/inventory-consumer';
import { FraudConsumer } from '../src/consumers/fraud-consumer';
import { ShippingConsumer } from '../src/consumers/shipping-consumer';
import { NotificationConsumer } from '../src/consumers/notification-consumer';
import { SagaOrchestrator } from '../src/saga/saga-orchestrator';
import { ProjectionConsumer } from '../src/application/projections/projection-consumer';
import { SocketRealtimeGateway } from '../src/realtime/realtime-gateway';
import { EventReplayService } from '../src/replay/event-replay-service';
import { ChaosEngine } from '../src/chaos/chaos-engine';
import { kafkaConfig, topics } from '../src/config';

import { BenchmarkContext } from './lib/benchmark-context';
import { runScenarioABaseline } from './scenarios/scenario-a-baseline';
import { runScenarioBNormal } from './scenarios/scenario-b-normal';
import { runScenarioCStress } from './scenarios/scenario-c-stress';
import { runScenarioDSpike } from './scenarios/scenario-d-spike';
import { runScenarioE2ELatency } from './scenarios/scenario-e2e-latency';
import { runScenarioOutbox } from './scenarios/scenario-outbox';
import { runScenarioSaga } from './scenarios/scenario-saga';
import { runScenarioRetryDLQ } from './scenarios/scenario-retry-dlq';
import { runResilienceConsumerRestart } from './scenarios/resilience-consumer-restart';
import { runResilienceOutboxRestart } from './scenarios/resilience-outbox-restart';
import { runResilienceKafkaRestart } from './scenarios/resilience-kafka-restart';
import { runResiliencePostgresRestart } from './scenarios/resilience-postgres-restart';

interface EmbeddedAppInstance {
  server: http.Server;
  db: DatabaseService;
  outboxRelay: OutboxRelay;
  consumers: any[];
  replayService: EventReplayService;
  stop: () => Promise<void>;
}

async function startEmbeddedApp(port = 3000): Promise<EmbeddedAppInstance> {
  const db = new DatabaseService();
  const outboxRelay = new OutboxRelay(db, { batchSize: 50, pollIntervalMs: 50 });
  const orderService = new OrderService(db, outboxRelay);

  const paymentConsumer = new PaymentConsumer(db);
  const inventoryConsumer = new InventoryConsumer(db);
  const fraudConsumer = new FraudConsumer(db);
  const shippingConsumer = new ShippingConsumer(db);
  const notificationConsumer = new NotificationConsumer(db);
  const sagaOrchestrator = new SagaOrchestrator(db);

  const server = http.createServer();
  const io = new Server(server, { cors: { origin: '*' } });
  const realtimeGateway = new SocketRealtimeGateway(io);
  const projectionConsumer = new ProjectionConsumer(realtimeGateway, db);
  const replayService = new EventReplayService(db, projectionConsumer);

  function getConsumersStatus() {
    return {
      payment: paymentConsumer.metrics,
      inventory: inventoryConsumer.metrics,
      fraud: fraudConsumer.metrics,
      shipping: shippingConsumer.metrics,
      notification: notificationConsumer.metrics,
      saga: sagaOrchestrator.metrics,
      projection: projectionConsumer.metrics,
    };
  }

  async function isKafkaReady() {
    const admin = new Kafka(kafkaConfig).admin();
    try {
      await admin.connect();
      await admin.listTopics();
      return true;
    } catch {
      return false;
    } finally {
      await admin.disconnect();
    }
  }

  const app = createApp({
    orderService,
    db,
    replayService,
    chaosEngine: ChaosEngine.getInstance(),
    getConsumersStatus,
    isKafkaReady,
  });

  server.on('request', app);

  // Initialize DB and Kafka topics
  await db.initialize();

  const admin = new Kafka(kafkaConfig).admin();
  await admin.connect();
  try {
    const existingTopics = await admin.listTopics();
    const topicsToCreate = Object.values(topics)
      .filter((t) => !existingTopics.includes(t.name))
      .map((t: any) => ({
        topic: t.name,
        numPartitions: t.partitions,
        replicationFactor: t.replicationFactor,
      }));

    if (topicsToCreate.length > 0) {
      await admin.createTopics({ waitForLeaders: true, topics: topicsToCreate });
    }
  } finally {
    await admin.disconnect();
  }

  await replayService.connect();
  await outboxRelay.start();

  const consumers = [
    paymentConsumer,
    inventoryConsumer,
    fraudConsumer,
    shippingConsumer,
    notificationConsumer,
    sagaOrchestrator,
    projectionConsumer,
  ];

  await Promise.all(consumers.map((c) => c.start()));

  await new Promise<void>((resolve) => {
    server.listen(port, () => resolve());
  });

  return {
    server,
    db,
    outboxRelay,
    consumers,
    replayService,
    stop: async () => {
      server.close();
      io.close();
      await Promise.allSettled([
        outboxRelay.stop(),
        ...consumers.map((c) => c.stop()),
        replayService.disconnect(),
      ]);
      await db.disconnect();
    },
  };
}

async function main() {
  const args = process.argv.slice(2);
  const scenarioArg = args.find((a) => a.startsWith('--scenario='))?.split('=')[1] || '';
  const suiteArg = args.find((a) => a.startsWith('--suite='))?.split('=')[1] || (args.includes('--all') ? 'all' : '');

  const ctx = new BenchmarkContext('localhost', 3000);
  let embeddedApp: EmbeddedAppInstance | null = null;

  // Check if app is already running
  const isAlreadyRunning = await ctx.waitForAppReady(3);
  if (!isAlreadyRunning) {
    console.log('>> Application not running on port 3000. Launching embedded benchmark instance...');
    embeddedApp = await startEmbeddedApp(3000);
    const ready = await ctx.waitForAppReady(15);
    if (!ready) {
      console.error('>> ERROR: Failed to start application on port 3000');
      process.exit(1);
    }
    console.log('>> Embedded application started and ready on port 3000.');
    await new Promise((resolve) => setTimeout(resolve, 3000));
  } else {
    console.log('>> Detected existing application running and ready on port 3000.');
  }

  const results: Record<string, any> = {};

  try {
    if (scenarioArg === 'baseline' || suiteArg === 'performance' || suiteArg === 'all' || (!scenarioArg && !suiteArg)) {
      results.baseline = await runScenarioABaseline(ctx);
    }
    if (scenarioArg === 'normal' || suiteArg === 'performance' || suiteArg === 'all') {
      results.normal = await runScenarioBNormal(ctx);
    }
    if (scenarioArg === 'stress' || suiteArg === 'performance' || suiteArg === 'all') {
      results.stress = await runScenarioCStress(ctx);
    }
    if (scenarioArg === 'spike' || suiteArg === 'performance' || suiteArg === 'all') {
      results.spike = await runScenarioDSpike(ctx);
    }
    if (scenarioArg === 'e2e-latency' || suiteArg === 'performance' || suiteArg === 'all') {
      results.e2eLatency = await runScenarioE2ELatency(ctx);
    }
    if (scenarioArg === 'outbox' || suiteArg === 'performance' || suiteArg === 'all') {
      results.outbox = await runScenarioOutbox(ctx);
    }
    if (scenarioArg === 'saga' || suiteArg === 'performance' || suiteArg === 'all') {
      results.saga = await runScenarioSaga(ctx);
    }
    if (scenarioArg === 'retry-dlq' || suiteArg === 'performance' || suiteArg === 'all') {
      results.retryDLQ = await runScenarioRetryDLQ(ctx);
    }
    if (scenarioArg === 'resilience-consumer' || suiteArg === 'resilience' || suiteArg === 'all') {
      results.resilienceConsumer = await runResilienceConsumerRestart(ctx);
    }
    if (scenarioArg === 'resilience-outbox' || suiteArg === 'resilience' || suiteArg === 'all') {
      results.resilienceOutbox = await runResilienceOutboxRestart(ctx);
    }
    if (scenarioArg === 'resilience-kafka' || suiteArg === 'resilience' || suiteArg === 'all') {
      results.resilienceKafka = await runResilienceKafkaRestart(ctx);
    }
    if (scenarioArg === 'resilience-postgres' || suiteArg === 'resilience' || suiteArg === 'all') {
      results.resiliencePostgres = await runResiliencePostgresRestart(ctx);
    }

    // Save benchmark baseline summary model (Phase 16)
    if (results.baseline || results.normal) {
      const baselineModel = {
        commit: '41445c3',
        date: new Date().toISOString(),
        environment: ctx.getEnvironmentInfo(),
        throughput: {
          baselineReqPerSec: results.baseline?.httpMetrics?.actualThroughputReqPerSec || null,
          normalReqPerSec: results.normal?.httpMetrics?.actualThroughputReqPerSec || null,
          stressMaxSustainableReqPerSec: results.stress?.summary?.maxSustainableObservedThroughputReqPerSec || null,
        },
        latencyMs: {
          httpP50: results.normal?.httpMetrics?.latency?.p50 ?? results.baseline?.httpMetrics?.latency?.p50,
          httpP95: results.normal?.httpMetrics?.latency?.p95 ?? results.baseline?.httpMetrics?.latency?.p95,
          httpP99: results.normal?.httpMetrics?.latency?.p99 ?? results.baseline?.httpMetrics?.latency?.p99,
          e2eP50: results.e2eLatency?.endToEndFulfillmentLatency?.p50 ?? results.normal?.e2eLatency?.fulfillmentLatency?.p50,
          e2eP95: results.e2eLatency?.endToEndFulfillmentLatency?.p95 ?? results.normal?.e2eLatency?.fulfillmentLatency?.p95,
          e2eP99: results.e2eLatency?.endToEndFulfillmentLatency?.p99 ?? results.normal?.e2eLatency?.fulfillmentLatency?.p99,
        },
        errorRatePercent: results.normal?.httpMetrics?.errorRatePercent ?? results.baseline?.httpMetrics?.errorRatePercent,
        consumerLag: {
          baselineFinalLag: results.baseline?.kafkaMetrics?.finalTotalLag,
          normalFinalLag: results.normal?.kafkaMetrics?.finalTotalLag,
          spikePeakLag: results.spike?.recoveryMetrics?.peakClusterLag,
        },
        recoveryTimeSec: {
          consumerLagRecovery: results.spike?.recoveryMetrics?.lagRecoveryTimeSec,
          outboxDrainRecovery: results.spike?.recoveryMetrics?.outboxDrainTimeSec,
          kafkaRestartRecovery: results.resilienceKafka?.totalRecoveryTimeSec,
          postgresRestartRecovery: results.resiliencePostgres?.totalRecoveryTimeSec,
        },
      };

      fs.writeFileSync(
        path.join(process.cwd(), 'benchmark-baseline.json'),
        JSON.stringify(baselineModel, null, 2),
        'utf8'
      );
    }

    console.log('\n===============================================================');
    console.log(' BENCHMARK EXECUTION COMPLETED SUCCESSFULLY');
    console.log(' Results persisted in benchmark-results/');
    console.log('===============================================================\n');
  } finally {
    if (embeddedApp) {
      console.log('>> Shutting down embedded benchmark application...');
      await embeddedApp.stop();
    }
    await ctx.cleanup();
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error('Fatal benchmark error:', err);
    process.exit(1);
  });
}
