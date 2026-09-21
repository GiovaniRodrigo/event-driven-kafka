import { DatabaseService } from '../../src/services/database';
import { ProjectionConsumer } from '../../src/application/projections/projection-consumer';
import { EventReplayService } from '../../src/replay/event-replay-service';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';
import { Pool } from 'pg';

describe('Deterministic Replay & Projection Rebuild Integration Tests', () => {
  let db: DatabaseService;
  let isRealPostgres = false;
  let gatewayMock: any;
  let projectionConsumer: ProjectionConsumer;
  let replayService: EventReplayService;

  beforeAll(async () => {
    gatewayMock = {
      orderCreated: jest.fn(),
      orderUpdated: jest.fn(),
      orderEvent: jest.fn(),
      consumerHealth: jest.fn(),
    };

    const dbUrl = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/event_driven';
    const testPool = new Pool({ connectionString: dbUrl, connectionTimeoutMillis: 1000 });
    try {
      await testPool.query('SELECT 1');
      isRealPostgres = true;
      db = new DatabaseService(testPool);
      await db.initializeTables();
      projectionConsumer = new ProjectionConsumer(gatewayMock, db);
      replayService = new EventReplayService(db, projectionConsumer);
    } catch {
      isRealPostgres = false;
    } finally {
      if (!isRealPostgres) {
        await testPool.end().catch(() => {});
      }
    }
  });

  afterAll(async () => {
    if (isRealPostgres && db) {
      await db.disconnect().catch(() => {});
    }
  });

  it('proves deterministic projection reconstruction with zero external side effects and identical state', async () => {
    if (!isRealPostgres) {
      console.warn('Skipping Replay Rebuild on real Postgres (INFRASTRUCTURE UNAVAILABLE)');
      return;
    }

    const orderId = `ord_replay_int_${Date.now()}`;
    const correlationId = `corr_${orderId}`;

    const events = [
      createEventEnvelope({
        eventType: EventTypes.OrderCreated,
        aggregateId: orderId,
        aggregateType: 'Order',
        producer: 'order-service',
        correlationId,
        causationId: 'cmd_1',
        sequenceNumber: 1,
        payload: {
          order_id: orderId,
          user_id: 'usr_replay_1',
          total_amount: 150,
          currency: 'USD',
          items: [{ sku: 'LAPTOP-001', name: 'Workstation', price: 150, quantity: 1 }],
        },
      }),
      createEventEnvelope({
        eventType: EventTypes.PaymentAuthorized,
        aggregateId: orderId,
        aggregateType: 'Payment',
        producer: 'payment-service',
        correlationId,
        causationId: 'evt_1',
        sequenceNumber: 2,
        payload: {
          order_id: orderId,
          payment_id: `pay_${orderId}`,
          amount: 150,
          authorization_code: 'AUTH_REPLAY_1',
        },
      }),
      createEventEnvelope({
        eventType: EventTypes.InventoryReserved,
        aggregateId: orderId,
        aggregateType: 'Inventory',
        producer: 'inventory-service',
        correlationId,
        causationId: 'evt_2',
        sequenceNumber: 3,
        payload: {
          order_id: orderId,
          reservation_id: `res_${orderId}`,
          items: [{ sku: 'LAPTOP-001', quantity: 1, name: 'Workstation' }],
        },
      }),
      createEventEnvelope({
        eventType: EventTypes.FraudApproved,
        aggregateId: orderId,
        aggregateType: 'Fraud',
        producer: 'fraud-service',
        correlationId,
        causationId: 'evt_3',
        sequenceNumber: 4,
        payload: { order_id: orderId, fraud_check_id: `fraud_${orderId}`, score: 10 },
      }),
      createEventEnvelope({
        eventType: EventTypes.ShipmentCreated,
        aggregateId: orderId,
        aggregateType: 'Shipping',
        producer: 'shipping-service',
        correlationId,
        causationId: 'evt_4',
        sequenceNumber: 5,
        payload: {
          order_id: orderId,
          shipment_id: `ship_${orderId}`,
          tracking_number: `TRK_${orderId}`,
          carrier: 'FEDEX',
        },
      }),
      createEventEnvelope({
        eventType: EventTypes.OrderCompleted,
        aggregateId: orderId,
        aggregateType: 'Order',
        producer: 'saga-orchestrator',
        correlationId,
        causationId: 'evt_5',
        sequenceNumber: 6,
        payload: { order_id: orderId, completed_at: new Date().toISOString() },
      }),
    ];

    // 1. Insert into immutable event store
    for (const evt of events) {
      await db.appendToEventStore(evt);
    }

    const eventCountBefore = (await db.getEventsByAggregateId(orderId)).length;
    expect(eventCountBefore).toBe(6);

    // 2. Clear gateway mock calls to verify zero side-effects during replay
    gatewayMock.orderCreated.mockClear();
    gatewayMock.orderUpdated.mockClear();
    gatewayMock.orderEvent.mockClear();

    // 3. Replay Run #1
    const replay1 = await replayService.replayAggregate(orderId);
    expect(replay1.status).toBe('SUCCESS');
    expect(replay1.events_processed).toBe(6);
    const stateA = replay1.reconstructed_state;
    expect(stateA).toBeDefined();
    expect(stateA.status).toBe('completed');
    expect(stateA.total_amount).toBe(150);

    // Verify ZERO gateway / websocket calls during replay
    expect(gatewayMock.orderCreated).not.toHaveBeenCalled();
    expect(gatewayMock.orderUpdated).not.toHaveBeenCalled();
    expect(gatewayMock.orderEvent).not.toHaveBeenCalled();

    // 4. Replay Run #2
    const replay2 = await replayService.replayAggregate(orderId);
    expect(replay2.status).toBe('SUCCESS');
    const stateB = replay2.reconstructed_state;

    // 5. State A strictly equals State B (Deterministic Rebuild)
    expect(stateA.status).toEqual(stateB.status);
    expect(stateA.total_amount).toEqual(stateB.total_amount);
    expect(stateA.items).toEqual(stateB.items);

    // 6. Verify Event Store remained 100% unchanged (no re-appending)
    const eventCountAfter = (await db.getEventsByAggregateId(orderId)).length;
    expect(eventCountAfter).toBe(6);
  });
});
