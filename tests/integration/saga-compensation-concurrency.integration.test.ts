import { DatabaseService } from '../../src/services/database';
import { SagaOrchestrator } from '../../src/saga/saga-orchestrator';
import { createEventEnvelope } from '../../src/contracts/envelope';
import { EventTypes } from '../../src/contracts';
import { Pool } from 'pg';

describe('Saga Compensation Concurrency & Barrier Atomicity Integration Tests', () => {
  let db: DatabaseService;
  let isRealPostgres = false;
  let orchestrator: SagaOrchestrator;
  let emittedEvents: any[];

  beforeAll(async () => {
    emittedEvents = [];
    const dbUrl = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/event_driven';
    const testPool = new Pool({ connectionString: dbUrl, connectionTimeoutMillis: 1000 });
    try {
      await testPool.query('SELECT 1');
      isRealPostgres = true;
      db = new DatabaseService(testPool);
      await db.initializeTables();
      orchestrator = new SagaOrchestrator(db);
      (orchestrator as any).emit = jest.fn().mockImplementation(async (params) => {
        emittedEvents.push(params);
        return params;
      });
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

  it('handles concurrent compensation completions atomically and emits exactly one OrderCancelled event', async () => {
    if (!isRealPostgres) {
      console.warn('Skipping Saga Concurrency on real Postgres (INFRASTRUCTURE UNAVAILABLE)');
      return;
    }

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

    // 5. Late/duplicate compensation event arrives
    const duplicateRefund = createEventEnvelope({
      eventType: EventTypes.PaymentRefunded,
      aggregateId: orderId,
      aggregateType: 'Payment',
      producer: 'payment-service',
      correlationId,
      causationId: 'evt_refund_dup',
      payload: { order_id: orderId, payment_id: `pay_${orderId}` },
    });

    await (orchestrator as any).processEvent(duplicateRefund);

    // State remains CANCELLED and NO second OrderCancelled event is emitted
    const afterDupSaga = await db.getSagaByAggregateId(orderId);
    expect(afterDupSaga?.state).toBe('CANCELLED');

    const orderCancelledAfterDup = emittedEvents.filter(
      (e) => e.eventType === EventTypes.OrderCancelled && e.aggregateId === orderId
    );
    expect(orderCancelledAfterDup.length).toBe(1);
  });
});
