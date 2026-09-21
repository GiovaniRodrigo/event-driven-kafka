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
    const admin = kafka.admin();
    await admin.connect();

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

    let message1Offset: string = '';
    let message1Partition: number = 0;

    await consumerInstance1.run({
      autoCommit: false,
      eachMessage: async ({ message, partition }) => {
        if (message.value) {
          receivedBatch1.push(JSON.parse(message.value.toString()));
          message1Offset = message.offset;
          message1Partition = partition;
          const targetOffset = (BigInt(message.offset) + 1n).toString();
          // Explicit offset commit of message 1
          await consumerInstance1.commitOffsets([
            { topic: testTopic2, partition, offset: targetOffset },
          ]);
          resolveBatch1();
        }
      },
    });

    // 1. Send message 1
    await producer.send({
      topic: testTopic2,
      messages: [{ key: orderId1, value: JSON.stringify(envelope1) }],
    });

    // 2. Consumer 1 receives message 1 and explicitly commits offset
    await batch1Promise;
    expect(receivedBatch1.length).toBe(1);
    expect(receivedBatch1[0].aggregate_id).toBe(orderId1);

    // 3. Verify via Kafka Admin API that Kafka has persisted that committed group offset
    const expectedOffset1 = (BigInt(message1Offset) + 1n).toString();
    const fetchedOffsets1 = await admin.fetchOffsets({ groupId, topics: [testTopic2] });
    const partitionOffsetObj1 = fetchedOffsets1
      .find((t) => t.topic === testTopic2)
      ?.partitions.find((p) => p.partition === message1Partition);

    expect(partitionOffsetObj1).toBeDefined();
    expect(partitionOffsetObj1?.offset).toBe(expectedOffset1);

    // 4. Only after asserting committed offset persistence in Kafka, disconnect Consumer 1
    await consumerInstance1.stop();
    await consumerInstance1.disconnect();

    // 5. Start consumer 2 with the same groupId
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

    let message2Offset: string = '';
    let message2Partition: number = 0;

    await consumerInstance2.run({
      autoCommit: false,
      eachMessage: async ({ message, partition }) => {
        if (message.value) {
          receivedBatch2.push(JSON.parse(message.value.toString()));
          message2Offset = message.offset;
          message2Partition = partition;
          const targetOffset = (BigInt(message.offset) + 1n).toString();
          await consumerInstance2.commitOffsets([
            { topic: testTopic2, partition, offset: targetOffset },
          ]);
          resolveBatch2();
        }
      },
    });

    // 6. Send message 2 to the topic
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

    // 7. Consumer 2 receives message 2
    await batch2Promise;

    // 8. Verify via Kafka Admin API that committed offset has advanced for message 2
    const expectedOffset2 = (BigInt(message2Offset) + 1n).toString();
    const fetchedOffsets2 = await admin.fetchOffsets({ groupId, topics: [testTopic2] });
    const partitionOffsetObj2 = fetchedOffsets2
      .find((t) => t.topic === testTopic2)
      ?.partitions.find((p) => p.partition === message2Partition);

    expect(partitionOffsetObj2).toBeDefined();
    expect(partitionOffsetObj2?.offset).toBe(expectedOffset2);
    expect(BigInt(expectedOffset2)).toBeGreaterThan(BigInt(expectedOffset1));

    // 9. Assertions prove:
    // - message 1 was committed before shutdown and not replayed to consumer 2
    // - message 2 was consumed and committed after restart
    expect(receivedBatch2.length).toBe(1);
    expect(receivedBatch2[0].aggregate_id).toBe(orderId2);

    await consumerInstance2.stop();
    await consumerInstance2.disconnect();
    await admin.disconnect();
  });
});
