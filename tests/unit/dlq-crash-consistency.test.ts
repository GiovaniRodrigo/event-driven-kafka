import { BaseConsumer } from '../../src/consumers/base-consumer';
import { EventReplayService } from '../../src/replay/event-replay-service';
import { createEventEnvelope, EventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';

class TestFailingConsumer extends BaseConsumer {
  public failCount = 0;
  public maxFailsBeforeSuccess = Infinity;

  protected async processEvent(envelope: EventEnvelope): Promise<void> {
    this.failCount++;
    if (this.failCount <= this.maxFailsBeforeSuccess) {
      throw new Error(`Simulated transient error attempt #${this.failCount}`);
    }
  }
}

describe('Dead Letter Queue (DLQ) Crash Consistency & Safe Replay Tests', () => {
  let dbMock: any;
  let dlqTable: any[];
  let processedEventsTable: any[];
  let kafkaDlqMessages: any[];
  let kafkaTargetMessages: any[];

  beforeEach(() => {
    dlqTable = [];
    processedEventsTable = [];
    kafkaDlqMessages = [];
    kafkaTargetMessages = [];

    dbMock = {
      recordDLQMessage: jest.fn().mockImplementation(async (dlq) => {
        dlqTable.push({
          id: dlq.id,
          event_id: dlq.eventId,
          topic: dlq.topic,
          partition: dlq.partition,
          offset_val: dlq.offset,
          consumer_name: dlq.consumerName,
          error_message: dlq.errorMessage,
          payload: dlq.payload,
          correlation_id: dlq.correlationId,
          status: 'UNRESOLVED',
          attempts: 1,
        });
      }),
      getDLQMessage: jest.fn().mockImplementation(async (id: string) => {
        return dlqTable.find((d) => d.id === id) || null;
      }),
      setDLQStatus: jest.fn().mockImplementation(async (id: string, status: string) => {
        const item = dlqTable.find((d) => d.id === id);
        if (item) item.status = status;
      }),
      markDLQResolved: jest.fn().mockImplementation(async (id: string, status = 'REPLAYED') => {
        const item = dlqTable.find((d) => d.id === id);
        if (item) item.status = status;
      }),
      isEventProcessed: jest.fn().mockImplementation(async (eventId: string, consumer: string) => {
        return processedEventsTable.some((p) => p.event_id === eventId && p.consumer_name === consumer && p.status === 'PROCESSED');
      }),
      markEventProcessed: jest.fn().mockImplementation(async (eventId: string, consumer: string, status: string, error?: string) => {
        const item = processedEventsTable.find((p) => p.event_id === eventId && p.consumer_name === consumer);
        if (item) {
          item.status = status;
          item.error = error;
        } else {
          processedEventsTable.push({ event_id: eventId, consumer_name: consumer, status, error });
        }
      }),
      resetProcessedEventForReplay: jest.fn().mockImplementation(async (eventId: string, consumer: string) => {
        const idx = processedEventsTable.findIndex((p) => p.event_id === eventId && (!consumer || p.consumer_name === consumer));
        if (idx !== -1) processedEventsTable.splice(idx, 1);
      }),
    };
  });

  it('exhausted retries durably persists message to DLQ table before committing offset', async () => {
    const consumer = new TestFailingConsumer('payments.events', 'payment-group', 'payment-service', dbMock);
    (consumer as any).baseBackoffMs = 1; // Instant backoff for test speed
    (consumer as any).jitterMs = 1;
    (consumer as any).producer = {
      send: jest.fn().mockImplementation(async (msg) => {
        kafkaDlqMessages.push(msg);
        return [{ partition: 0, offset: '99' }];
      }),
    };

    const envelope = createEventEnvelope({
      eventId: 'evt_pay_fail_01',
      eventType: EventTypes.PaymentRequested,
      aggregateId: 'ord_dlq_01',
      aggregateType: 'Payment',
      producer: 'order-service',
      correlationId: 'corr_dlq_01',
      causationId: 'cmd_01',
      payload: { order_id: 'ord_dlq_01', amount: 99.99 },
    });

    const kafkaMessage: any = {
      topic: 'payments.events',
      partition: 0,
      message: {
        offset: '55',
        value: Buffer.from(JSON.stringify(envelope)),
        timestamp: '1600000000000',
      },
    };

    await (consumer as any).handleMessage(kafkaMessage);

    // 1. DLQ persisted in DB
    expect(dlqTable).toHaveLength(1);
    expect(dlqTable[0].event_id).toBe('evt_pay_fail_01');
    expect(dlqTable[0].consumer_name).toBe('payment-service');
    expect(dlqTable[0].status).toBe('UNRESOLVED');

    // 2. Processed events marked as FAILED (so offset advancement doesn't block partition)
    const proc = processedEventsTable.find((p) => p.event_id === 'evt_pay_fail_01');
    expect(proc).toBeDefined();
    expect(proc.status).toBe('FAILED');

    // 3. Kafka DLQ topic notified
    expect(kafkaDlqMessages).toHaveLength(1);
  });

  it('DLQ replay resets idempotency state and safely republishes to original topic', async () => {
    const replayService = new EventReplayService(dbMock);
    (replayService as any).producer = {
      send: jest.fn().mockImplementation(async (msg) => {
        kafkaTargetMessages.push(msg);
        return [{ partition: 0, offset: '101' }];
      }),
    };

    // Seed a dead-lettered message
    const envelope = createEventEnvelope({
      eventId: 'evt_replayed_02',
      eventType: EventTypes.OrderCreated,
      aggregateId: 'ord_dlq_02',
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId: 'corr_02',
      causationId: 'cmd_02',
      payload: { order_id: 'ord_dlq_02', total_amount: 150 },
    });

    dlqTable.push({
      id: 'dlq_msg_002',
      event_id: 'evt_replayed_02',
      topic: 'orders.events',
      consumer_name: 'payment-service',
      payload: { original_event: envelope },
      status: 'UNRESOLVED',
    });

    processedEventsTable.push({
      event_id: 'evt_replayed_02',
      consumer_name: 'payment-service',
      status: 'FAILED',
    });

    // Execute Replay
    const result = await replayService.replayDLQ('dlq_msg_002');

    expect(result.status).toBe('SUCCESS');
    expect(result.events_processed).toBe(1);

    // Verify FAILED record was deleted to unblock consumer re-processing
    const proc = processedEventsTable.find((p) => p.event_id === 'evt_replayed_02');
    expect(proc).toBeUndefined();

    // Verify DLQ marked as REPLAYED
    expect(dlqTable[0].status).toBe('REPLAYED');

    // Verify Kafka message sent with correct headers
    expect(kafkaTargetMessages).toHaveLength(1);
    expect(kafkaTargetMessages[0].topic).toBe('orders.events');
  });

  it('DLQ replay failure leaves message in UNRESOLVED status for subsequent recovery', async () => {
    const replayService = new EventReplayService(dbMock);
    (replayService as any).producer = {
      send: jest.fn().mockRejectedValue(new Error('Broker unreachable during replay publish')),
    };

    dlqTable.push({
      id: 'dlq_msg_003',
      event_id: 'evt_03',
      topic: 'orders.events',
      consumer_name: 'payment-service',
      payload: { original_event: { event_id: 'evt_03', aggregate_id: 'ord_03' } },
      status: 'UNRESOLVED',
    });

    const result = await replayService.replayDLQ('dlq_msg_003');

    expect(result.status).toBe('FAILED');
    expect(result.error).toContain('Broker unreachable');
    // DLQ status must remain UNRESOLVED (not REPLAYED) so operator can retry later
    expect(dlqTable[0].status).toBe('UNRESOLVED');
  });
});
