import { DatabaseService } from '../../src/services/database';
import { SagaOrchestrator } from '../../src/saga/saga-orchestrator';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';
import { Pool } from 'pg';

describe('Saga Compensation Concurrency & Barrier Atomicity Integration Tests (PostgreSQL)', () => {
  let db: DatabaseService;
  let pool: Pool;
  let orchestrator: SagaOrchestrator;
  let emittedEvents: any[];

  beforeAll(async () => {
    emittedEvents = [];
    const dbUrl = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/event_driven_test';
    pool = new Pool({ connectionString: dbUrl, connectionTimeoutMillis: 3000 });
    // This MUST fail loudly if PostgreSQL is unreachable
    await pool.query('SELECT 1');
    db = new DatabaseService(pool);
    await db.initializeTables();
    orchestrator = new SagaOrchestrator(db);
    (orchestrator as any).emit = jest.fn().mockImplementation(async (params) => {
      emittedEvents.push(params);
      return params;
    });
  });

  beforeEach(async () => {
    emittedEvents = [];
    await pool.query('TRUNCATE saga_instances RESTART IDENTITY CASCADE');
  });

  afterAll(async () => {
    if (db) {
      await db.disconnect().catch(() => {});
    }
  });

  it('handles concurrent compensation completions atomically and emits exactly one OrderCancelled event', async () => {
    const orderId = `ord_saga_conc_${Date.now()}`;
    const correlationId = `corr_${orderId}`;

    // 1. Seed Saga in COMPENSATING state with dual pending steps
    await db.saveSagaInstance({
      sagaId: `saga_${orderId}`,
      aggregateId: orderId,
      sagaType: 'ORDER_FULFILLMENT',
      state: 'COMPENSATING',
      currentStep: 'COMPENSATING_FRAUD',
      correlationId,
      context: {
        order_id: orderId,
        user_id: 'usr_conc_1',
        total_amount: 500,
        payment_id: `pay_${orderId}`,
        reservation_id: `res_${orderId}`,
        compensations_pending: ['INVENTORY_RELEASE', 'PAYMENT_REFUND'],
        compensations_completed: [],
      },
      failureReason: 'Fraud threshold exceeded',
    });

    // 2. Dispatch PaymentRefunded and InventoryReleased concurrently
    const paymentRefunded = createEventEnvelope({
      eventType: EventTypes.PaymentRefunded,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_refund',
      payload: { order_id: orderId, payment_id: `pay_${orderId}`, refund_id: `ref_${orderId}` },
    });

    const inventoryReleased = createEventEnvelope({
      eventType: EventTypes.InventoryReleased,
      aggregateId: orderId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId,
      causationId: 'evt_rel',
      payload: { order_id: orderId, reservation_id: `res_${orderId}`, items: [] },
    });

    await Promise.all([
      (orchestrator as any).processEvent(paymentRefunded),
      (orchestrator as any).processEvent(inventoryReleased),
    ]);

    // 3. Verify final state in PostgreSQL
    const finalSaga = await db.getSagaByAggregateId(orderId);
    expect(finalSaga).toBeDefined();
    expect(finalSaga?.state).toBe('CANCELLED');
    expect(finalSaga?.current_step).toBe('TERMINAL_CANCELLED');

    const completed = finalSaga?.context.compensations_completed as string[];
    expect(completed).toContain('PAYMENT_REFUND');
    expect(completed).toContain('INVENTORY_RELEASE');

    // 4. Verify EXACTLY ONE OrderCancelled was emitted
    const orderCancelledEvents = emittedEvents.filter(
      (e) => e.eventType === EventTypes.OrderCancelled && e.aggregateId === orderId
    );
    expect(orderCancelledEvents.length).toBe(1);

    // 5. Late/duplicate compensation events arrive
    const duplicateRefund = createEventEnvelope({
      eventType: EventTypes.PaymentRefunded,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_refund_dup',
      payload: { order_id: orderId, payment_id: `pay_${orderId}` },
    });

    const duplicateInventory = createEventEnvelope({
      eventType: EventTypes.InventoryReleased,
      aggregateId: orderId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId,
      causationId: 'evt_inv_dup',
      payload: { order_id: orderId, reservation_id: `res_${orderId}` },
    });

    await (orchestrator as any).processEvent(duplicateRefund);
    await (orchestrator as any).processEvent(duplicateInventory);

    // State remains CANCELLED and NO duplicate OrderCancelled events are emitted
    const afterDupSaga = await db.getSagaByAggregateId(orderId);
    expect(afterDupSaga?.state).toBe('CANCELLED');

    const orderCancelledAfterDup = emittedEvents.filter(
      (e) => e.eventType === EventTypes.OrderCancelled && e.aggregateId === orderId
    );
    expect(orderCancelledAfterDup.length).toBe(1);
  });

  it('handles sequential arrival in reverse order (InventoryReleased then PaymentRefunded)', async () => {
    const orderId = `ord_saga_rev_${Date.now()}`;
    const correlationId = `corr_${orderId}`;

    await db.saveSagaInstance({
      sagaId: `saga_${orderId}`,
      aggregateId: orderId,
      sagaType: 'ORDER_FULFILLMENT',
      state: 'COMPENSATING',
      currentStep: 'COMPENSATING_SHIPPING',
      correlationId,
      context: {
        order_id: orderId,
        user_id: 'usr_rev_1',
        total_amount: 300,
        payment_id: `pay_${orderId}`,
        reservation_id: `res_${orderId}`,
        compensations_pending: ['INVENTORY_RELEASE', 'PAYMENT_REFUND'],
        compensations_completed: [],
      },
      failureReason: 'Shipping carrier unavailable',
    });

    // Step 1: InventoryReleased first
    const inventoryReleased = createEventEnvelope({
      eventType: EventTypes.InventoryReleased,
      aggregateId: orderId,
      aggregateType: 'Inventory',
      producer: 'inventory-service',
      correlationId,
      causationId: 'evt_rev_1',
      payload: { order_id: orderId, reservation_id: `res_${orderId}`, items: [] },
    });
    await (orchestrator as any).processEvent(inventoryReleased);

    // Verify intermediate state in PostgreSQL: still COMPENSATING, no OrderCancelled emitted yet
    const midSaga = await db.getSagaByAggregateId(orderId);
    expect(midSaga?.state).toBe('COMPENSATING');
    expect(midSaga?.context.compensations_completed).toEqual(['INVENTORY_RELEASE']);
    expect(emittedEvents.filter((e) => e.eventType === EventTypes.OrderCancelled && e.aggregateId === orderId).length).toBe(0);

    // Step 2: PaymentRefunded second
    const paymentRefunded = createEventEnvelope({
      eventType: EventTypes.PaymentRefunded,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_rev_2',
      payload: { order_id: orderId, payment_id: `pay_${orderId}`, refund_id: `ref_${orderId}` },
    });
    await (orchestrator as any).processEvent(paymentRefunded);

    // Final state: CANCELLED, exactly 1 OrderCancelled
    const endSaga = await db.getSagaByAggregateId(orderId);
    expect(endSaga?.state).toBe('CANCELLED');
    expect(endSaga?.current_step).toBe('TERMINAL_CANCELLED');
    expect(emittedEvents.filter((e) => e.eventType === EventTypes.OrderCancelled && e.aggregateId === orderId).length).toBe(1);
  });
});
