import {
  createEventEnvelope,
  validateEventEnvelope,
  OrderCreatedPayloadSchema,
  PaymentRequestedPayloadSchema,
  InventoryReservationRequestedPayloadSchema,
  FraudCheckRequestedPayloadSchema,
  ShipmentRequestedPayloadSchema,
  EventTypes,
} from '../../src/contracts';

describe('Event Contracts & Envelope Unit Tests', () => {
  it('creates and validates a standard event envelope with valid payload', () => {
    const envelope = createEventEnvelope({
      eventType: EventTypes.OrderCreated,
      aggregateId: 'ord_123',
      aggregateType: 'Order',
      producer: 'order-service',
      correlationId: 'corr_123',
      causationId: 'cmd_123',
      payload: {
        order_id: 'ord_123',
        user_id: 'usr_456',
        items: [{ sku: 'LAPTOP-001', name: 'Workstation', price: 1500, quantity: 1 }],
        total_amount: 1500,
      },
    });

    expect(envelope.event_id).toBeDefined();
    expect(envelope.event_version).toBe(1);
    expect(envelope.event_type).toBe('OrderCreated');

    const validated = validateEventEnvelope(envelope, OrderCreatedPayloadSchema);
    expect(validated.payload.total_amount).toBe(1500);
  });

  it('rejects an invalid payload missing required fields', () => {
    expect(() => {
      OrderCreatedPayloadSchema.parse({
        order_id: 'ord_123',
        // missing user_id and items
        total_amount: -100, // invalid negative
      });
    }).toThrow();
  });

  it('validates PaymentRequested schema', () => {
    const valid = PaymentRequestedPayloadSchema.parse({
      order_id: 'ord_123',
      user_id: 'usr_1',
      amount: 250.5,
      currency: 'USD',
    });
    expect(valid.amount).toBe(250.5);
  });

  it('validates InventoryReservationRequested schema', () => {
    const valid = InventoryReservationRequestedPayloadSchema.parse({
      order_id: 'ord_123',
      items: [{ sku: 'SKU_1', name: 'Widget', price: 10, quantity: 2 }],
    });
    expect(valid.items).toHaveLength(1);
  });

  it('validates FraudCheckRequested schema', () => {
    const valid = FraudCheckRequestedPayloadSchema.parse({
      order_id: 'ord_123',
      user_id: 'usr_1',
      amount: 1200,
      payment_id: 'pay_999',
    });
    expect(valid.payment_id).toBe('pay_999');
  });

  it('validates ShipmentRequested schema', () => {
    const valid = ShipmentRequestedPayloadSchema.parse({
      order_id: 'ord_123',
      user_id: 'usr_1',
      items: [{ sku: 'SKU_1', name: 'Widget', price: 10, quantity: 1 }],
      shipping_address: { street: '123 Main' },
    });
    expect(valid.order_id).toBe('ord_123');
  });
});
