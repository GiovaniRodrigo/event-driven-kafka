import { Kafka, Producer, Consumer } from 'kafkajs';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';

describe('Kafka Broker Integration & At-Least-Once Messaging Tests', () => {
  const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:9092';
  let kafka: Kafka;
  let producer: Producer;
  let consumer: Consumer;
  const testTopic = `test.orders.events.${Date.now()}`;
  const dlqTopic = `test.platform.dlq.${Date.now()}`;

  beforeAll(async () => {
    kafka = new Kafka({
      clientId: `kafka-test-client-${Date.now()}`,
      brokers: [kafkaBroker],
      retry: { retries: 2, initialRetryTime: 300 },
    });

    // Fails loudly if Kafka broker is unavailable
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic: testTopic, numPartitions: 1, replicationFactor: 1 },
        { topic: dlqTopic, numPartitions: 1, replicationFactor: 1 },
      ],
    });
    await admin.disconnect();

    producer = kafka.producer({ allowAutoTopicCreation: true });
    await producer.connect();
  });

  afterAll(async () => {
    if (consumer) {
      await consumer.disconnect().catch(() => {});
    }
    if (producer) {
      await producer.disconnect().catch(() => {});
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
    consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic: testTopic, fromBeginning: true });

    const messagePromise = new Promise<void>((resolve) => {
      consumer.run({
        eachMessage: async ({ message }) => {
          if (message.value) {
            receivedMessages.push(JSON.parse(message.value.toString()));
            resolve();
          }
        },
      });
    });

    // Produce message
    await producer.send({
      topic: testTopic,
      messages: [{ key: orderId, value: JSON.stringify(envelope) }],
    });

    await messagePromise;

    expect(receivedMessages.length).toBeGreaterThanOrEqual(1);
    const matched = receivedMessages.find((m) => m.aggregate_id === orderId);
    expect(matched).toBeDefined();
    expect(matched.event_type).toBe(EventTypes.OrderCreated);
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
    let consumerInstance = kafka.consumer({ groupId });
    await consumerInstance.connect();
    await consumerInstance.subscribe({ topic: testTopic, fromBeginning: false });

    const receivedBatch1: any[] = [];
    let resolveBatch1: () => void;
    const batch1Promise = new Promise<void>((res) => {
      resolveBatch1 = res;
    });

    await consumerInstance.run({
      autoCommit: true,
      eachMessage: async ({ message }) => {
        if (message.value) {
          receivedBatch1.push(JSON.parse(message.value.toString()));
          resolveBatch1();
        }
      },
    });

    // Send message 1
    await producer.send({
      topic: testTopic,
      messages: [{ key: orderId, value: JSON.stringify(envelope) }],
    });

    await batch1Promise;
    expect(receivedBatch1.length).toBe(1);

    // Stop consumer 1 (simulated crash / restart)
    await consumerInstance.disconnect();

    // Start consumer 2 with same groupId
    const receivedBatch2: any[] = [];
    consumerInstance = kafka.consumer({ groupId });
    await consumerInstance.connect();
    await consumerInstance.subscribe({ topic: testTopic, fromBeginning: false });

    await consumerInstance.run({
      autoCommit: true,
      eachMessage: async ({ message }) => {
        if (message.value) {
          receivedBatch2.push(JSON.parse(message.value.toString()));
        }
      },
    });

    // Wait 1 second to confirm no duplicate replay of committed message 1
    await new Promise((r) => setTimeout(r, 1000));
    expect(receivedBatch2.length).toBe(0);

    await consumerInstance.disconnect();
  });
});
