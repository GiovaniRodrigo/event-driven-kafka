import { Kafka, Producer, Consumer, logLevel } from 'kafkajs';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';

jest.setTimeout(30000);

describe('Kafka Broker Integration & At-Least-Once Messaging Tests', () => {
  const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:9092';
  let kafka: Kafka;
  let producer: Producer;
  const activeConsumers: Consumer[] = [];
  const testTopic1 = `test.orders.events.${Date.now()}.1`;
  const testTopic2 = `test.orders.events.${Date.now()}.2`;
  const dlqTopic = `test.platform.dlq.${Date.now()}`;

  beforeAll(async () => {
    kafka = new Kafka({
      clientId: `kafka-test-client-${Date.now()}`,
      brokers: [kafkaBroker],
      logLevel: logLevel.NOTHING,
      connectionTimeout: 10000,
      requestTimeout: 25000,
      retry: { retries: 10, initialRetryTime: 300, maxRetryTime: 2000 },
    });

    // Fails loudly if Kafka broker is unavailable
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic: testTopic1, numPartitions: 1, replicationFactor: 1 },
        { topic: testTopic2, numPartitions: 1, replicationFactor: 1 },
        { topic: dlqTopic, numPartitions: 1, replicationFactor: 1 },
      ],
    });
    await admin.disconnect();

    producer = kafka.producer({ allowAutoTopicCreation: true });
    await producer.connect();
  });

  afterAll(async () => {
    for (const c of activeConsumers) {
      try {
        await c.stop();
        await c.disconnect();
      } catch {
        // Ignored on cleanup
      }
    }
    if (producer) {
      try {
        await producer.disconnect();
      } catch {
        // Ignored on cleanup
      }
    }
  });

  it('verifies produce, consume, and offset commit flow with real Kafka broker', async () => {
    const orderId = `ord_kafka_${Date.now()}`;
    const envelope = createEventEnvelope({
      eventType: EventTypes.OrderCreated,
      aggregateId: orderId,
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId: `corr_${orderId}`,
      causationId: 'cmd_kafka_1',
      payload: { order_id: orderId, total_amount: 250 },
    });

    const receivedMessages: any[] = [];
    const groupId = `test-group-${Date.now()}`;
    const consumer = kafka.consumer({
      groupId,
      retry: { retries: 10, initialRetryTime: 300 },
    });
    activeConsumers.push(consumer);

    await consumer.connect();
    await consumer.subscribe({ topic: testTopic1, fromBeginning: true });

    let resolveMessage: () => void;
    const messagePromise = new Promise<void>((resolve) => {
      resolveMessage = resolve;
    });

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (message.value) {
          receivedMessages.push(JSON.parse(message.value.toString()));
          resolveMessage();
        }
      },
    });

    // Produce message
    await producer.send({
      topic: testTopic1,
      messages: [{ key: orderId, value: JSON.stringify(envelope) }],
    });

    await messagePromise;

    expect(receivedMessages.length).toBeGreaterThanOrEqual(1);
    const matched = receivedMessages.find((m) => m.aggregate_id === orderId);
    expect(matched).toBeDefined();
    expect(matched.event_type).toBe(EventTypes.OrderCreated);

    await consumer.stop();
    await consumer.disconnect();
  });

  it('verifies consumer restart and offset resumption without duplicate processing of committed offsets', async () => {
    const orderId = `ord_restart_${Date.now()}`;
    const envelope = createEventEnvelope({
      eventType: EventTypes.PaymentAuthorized,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId: `corr_${orderId}`,
      causationId: 'evt_p1',
      payload: { order_id: orderId, payment_id: `pay_${orderId}` },
    });

    const groupId = `test-restart-group-${Date.now()}`;
    const consumerInstance1 = kafka.consumer({
      groupId,
      retry: { retries: 10, initialRetryTime: 300 },
    });
    activeConsumers.push(consumerInstance1);

    await consumerInstance1.connect();
    await consumerInstance1.subscribe({ topic: testTopic2, fromBeginning: true });

    const receivedBatch1: any[] = [];
    let resolveBatch1: () => void;
    const batch1Promise = new Promise<void>((res) => {
      resolveBatch1 = res;
    });

    await consumerInstance1.run({
      autoCommit: true,
      autoCommitInterval: 100,
      eachMessage: async ({ message }) => {
        if (message.value) {
          receivedBatch1.push(JSON.parse(message.value.toString()));
          resolveBatch1();
        }
      },
    });

    // Send message 1
    await producer.send({
      topic: testTopic2,
      messages: [{ key: orderId, value: JSON.stringify(envelope) }],
    });

    await batch1Promise;
    expect(receivedBatch1.length).toBe(1);

    // Wait for autoCommit to persist offset
    await new Promise((r) => setTimeout(r, 600));

    // Stop consumer 1 (simulated crash / restart)
    await consumerInstance1.stop();
    await consumerInstance1.disconnect();

    // Start consumer 2 with same groupId
    const receivedBatch2: any[] = [];
    const consumerInstance2 = kafka.consumer({
      groupId,
      retry: { retries: 10, initialRetryTime: 300 },
    });
    activeConsumers.push(consumerInstance2);

    await consumerInstance2.connect();
    await consumerInstance2.subscribe({ topic: testTopic2, fromBeginning: true });

    await consumerInstance2.run({
      autoCommit: true,
      eachMessage: async ({ message }) => {
        if (message.value) {
          receivedBatch2.push(JSON.parse(message.value.toString()));
        }
      },
    });

    // Wait 1.5 seconds to confirm no duplicate replay of committed message 1
    await new Promise((r) => setTimeout(r, 1500));
    expect(receivedBatch2.length).toBe(0);

    await consumerInstance2.stop();
    await consumerInstance2.disconnect();
  });
});
