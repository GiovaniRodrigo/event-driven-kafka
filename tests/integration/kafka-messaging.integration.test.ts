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
    const orderId1 = `ord_restart_1_${Date.now()}`;
    const envelope1 = createEventEnvelope({
      eventType: EventTypes.PaymentAuthorized,
      aggregateId: orderId1,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId: `corr_${orderId1}`,
      causationId: 'evt_p1',
      payload: { order_id: orderId1, payment_id: `pay_${orderId1}` },
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
      autoCommit: false,
      eachMessage: async ({ message, partition }) => {
        if (message.value) {
          receivedBatch1.push(JSON.parse(message.value.toString()));
          // Explicit offset commit of message 1
          await consumerInstance1.commitOffsets([
            { topic: testTopic2, partition, offset: (BigInt(message.offset) + 1n).toString() },
          ]);
          resolveBatch1();
        }
      },
    });

    // Send message 1
    await producer.send({
      topic: testTopic2,
      messages: [{ key: orderId1, value: JSON.stringify(envelope1) }],
    });

    await batch1Promise;
    expect(receivedBatch1.length).toBe(1);

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

    let resolveBatch2: () => void;
    const batch2Promise = new Promise<void>((res) => {
      resolveBatch2 = res;
    });

    await consumerInstance2.run({
      autoCommit: false,
      eachMessage: async ({ message, partition }) => {
        if (message.value) {
          receivedBatch2.push(JSON.parse(message.value.toString()));
          await consumerInstance2.commitOffsets([
            { topic: testTopic2, partition, offset: (BigInt(message.offset) + 1n).toString() },
          ]);
          resolveBatch2();
        }
      },
    });

    // Send message 2 to the topic
    const orderId2 = `ord_restart_2_${Date.now()}`;
    const envelope2 = createEventEnvelope({
      eventType: EventTypes.PaymentAuthorized,
      aggregateId: orderId2,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId: `corr_${orderId2}`,
      causationId: 'evt_p2',
      payload: { order_id: orderId2, payment_id: `pay_${orderId2}` },
    });

    await producer.send({
      topic: testTopic2,
      messages: [{ key: orderId2, value: JSON.stringify(envelope2) }],
    });

    await batch2Promise;

    // Consumer 2 resumed from committed offset: received ONLY message 2 (no duplicate of message 1)
    expect(receivedBatch2.length).toBe(1);
    expect(receivedBatch2[0].aggregate_id).toBe(orderId2);

    await consumerInstance2.stop();
    await consumerInstance2.disconnect();
  });
});
