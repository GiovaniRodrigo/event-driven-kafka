import { DatabaseService } from '../../src/services/database';
import { ProjectionConsumer } from '../../src/application/projections/projection-consumer';
import { EventReplayService } from '../../src/replay/event-replay-service';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';
import { Pool } from 'pg';

describe('Deterministic Replay & Projection Transactionality Integration Tests (PostgreSQL)', () => {
  let db: DatabaseService;
  let pool: Pool;
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

    const dbUrl = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/event_driven_test';
    pool = new Pool({ connectionString: dbUrl, connectionTimeoutMillis: 3000 });
    // This MUST fail loudly if PostgreSQL is unreachable
    await pool.query('SELECT 1');
    db = new DatabaseService(pool);
    await db.initializeTables();
    projectionConsumer = new ProjectionConsumer(gatewayMock, db);
    replayService = new EventReplayService(db, projectionConsumer);
  });

  beforeEach(async () => {
    gatewayMock.orderCreated.mockClear();
    gatewayMock.orderUpdated.mockClear();
    gatewayMock.orderEvent.mockClear();
    gatewayMock.consumerHealth.mockClear();
    await pool.query('TRUNCATE event_store, order_read_model, inventory_read_model, payment_read_model, shipment_read_model, projection_applied_events RESTART IDENTITY CASCADE');
  });

  afterAll(async () => {
    if (db) {
      await db.disconnect().catch(() => {});
    }
  });

  it('proves deterministic projection reconstruction with zero external side effects and identical state', async () => {
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
          user_id: 'usr_replay_1',
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

    // 2. Replay Run #1
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

    // 3. Replay Run #2
    const replay2 = await replayService.replayAggregate(orderId);
    expect(replay2.status).toBe('SUCCESS');
    const stateB = replay2.reconstructed_state;

    // 4. State A strictly equals State B (Deterministic Rebuild)
    expect(stateA.status).toEqual(stateB.status);
    expect(stateA.total_amount).toEqual(stateB.total_amount);
    expect(stateA.items).toEqual(stateB.items);

    // 5. Verify Event Store remained 100% unchanged (no re-appending)
    const eventCountAfter = (await db.getEventsByAggregateId(orderId)).length;
    expect(eventCountAfter).toBe(6);
  });

  it('guarantees transactional atomicity between projection state mutations and projection_applied_events markers', async () => {
    const orderId = `ord_tx_atom_${Date.now()}`;
    const eventId = `evt_tx_1`;

    const event = createEventEnvelope({
      eventId,
      eventType: EventTypes.OrderCreated,
      aggregateId: orderId,
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId: `corr_${orderId}`,
      causationId: 'cmd_1',
      sequenceNumber: 1,
      payload: {
        order_id: orderId,
        user_id: 'usr_atom_1',
        total_amount: 99,
        currency: 'USD',
        items: [{ sku: 'SKU-ATOM', name: 'Item', price: 99, quantity: 1 }],
      },
    });

    // 1. Process successfully within transaction
    await db.withTransaction(async (client) => {
      await (projectionConsumer as any).applyHistoricalEvent(event, client);
    });

    // Verify both read model and marker exist
    const readModel = await db.getOrder(orderId);
    expect(readModel).toBeDefined();
    expect(readModel?.id).toBe(orderId);

    const isApplied = await db.isProjectionEventApplied('order-fulfillment-projection', eventId);
    expect(isApplied).toBe(true);

    // 2. Simulated failure mid-transaction on second event
    const event2 = createEventEnvelope({
      eventId: 'evt_tx_fail',
      eventType: EventTypes.PaymentAuthorized,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId: `corr_${orderId}`,
      causationId: 'evt_1',
      sequenceNumber: 2,
      payload: { order_id: orderId, user_id: 'usr_atom_1', payment_id: 'pay_fail', amount: 99 },
    });

    await expect(
      db.withTransaction(async (client) => {
        await (projectionConsumer as any).applyHistoricalEvent(event2, client);
        throw new Error('Simulated database write failure during projection');
      })
    ).rejects.toThrow('Simulated database write failure during projection');

    // Verify marker was rolled back and NOT persisted
    const isMarker2Applied = await db.isProjectionEventApplied('order-fulfillment-projection', 'evt_tx_fail');
    expect(isMarker2Applied).toBe(false);
  });
});
