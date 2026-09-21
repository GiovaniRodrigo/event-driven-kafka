import { PaymentConsumer } from '../../src/consumers/payment-consumer';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';

describe('Idempotency & Retry Unit Tests', () => {
  let dbMock: any;
  let processedEvents: Set<string>;
  let consumer: PaymentConsumer;

  beforeEach(() => {
    processedEvents = new Set();

    dbMock = {
      isEventProcessed: jest.fn().mockImplementation(async (eventId, consumerName) => {
        return processedEvents.has(`${eventId}:${consumerName}`);
      }),
      markEventProcessed: jest.fn().mockImplementation(async (eventId, consumerName) => {
        processedEvents.add(`${eventId}:${consumerName}`);
      }),
    };

    consumer = new PaymentConsumer(dbMock as any);
    (consumer as any).producer = {
      send: jest.fn().mockResolvedValue([{ partition: 0, offset: '1' }]),
    };
  });

  it('skips processing if event was already processed by this consumer', async () => {
    const envelope = createEventEnvelope({
      eventId: 'evt_dup_1',
      eventType: EventTypes.PaymentRequested,
      aggregateId: 'ord_1',
      aggregateType: 'Payment',
      producer: 'saga-orchestrator',
      correlationId: 'corr_1',
      causationId: 'cmd_1',
      payload: { order_id: 'ord_1', user_id: 'usr_1', amount: 100 },
    });

    processedEvents.add('evt_dup_1:payment-service');

    const processSpy = jest.spyOn(consumer as any, 'processEvent');

    await (consumer as any).handleMessage({
      topic: 'payments.events',
      partition: 0,
      message: { offset: '10', value: Buffer.from(JSON.stringify(envelope)) },
    });

    expect(processSpy).not.toHaveBeenCalled();
  });

  it('successfully processes new event and registers it in processed_events', async () => {
    const envelope = createEventEnvelope({
      eventId: 'evt_new_1',
      eventType: EventTypes.PaymentRequested,
      aggregateId: 'ord_1',
      aggregateType: 'Payment',
      producer: 'saga-orchestrator',
      correlationId: 'corr_1',
      causationId: 'cmd_1',
      payload: { order_id: 'ord_1', user_id: 'usr_1', amount: 100 },
    });

    await (consumer as any).handleMessage({
      topic: 'payments.events',
      partition: 0,
      message: { offset: '10', value: Buffer.from(JSON.stringify(envelope)) },
    });

    expect(processedEvents.has('evt_new_1:payment-service')).toBe(true);
  });
});
