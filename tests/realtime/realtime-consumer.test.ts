import { RealtimeConsumer } from '../../src/realtime/realtime-consumer';
import { RealtimeGateway } from '../../src/realtime/realtime-gateway';

function mockGateway(): jest.Mocked<RealtimeGateway> {
  return {
    orderCreated: jest.fn(),
    orderUpdated: jest.fn(),
    orderEvent: jest.fn(),
    consumerHealth: jest.fn(),
  };
}

describe('RealtimeConsumer bridge', () => {
  it('translates order.created into orderCreated + orderEvent broadcasts', () => {
    const gateway = mockGateway();
    const consumer = new RealtimeConsumer(gateway);

    consumer.handleEvent(
      JSON.stringify({
        type: 'order.created',
        order_id: 'ord_1',
        user_id: 'user_1',
        total_amount: 42,
        timestamp: '2026-01-01T00:00:00.000Z',
      })
    );

    expect(gateway.orderCreated).toHaveBeenCalledWith(
      expect.objectContaining({ order_id: 'ord_1', user_id: 'user_1', status: 'pending', total_amount: 42 })
    );
    expect(gateway.orderEvent).toHaveBeenCalledWith(
      'ord_1',
      expect.objectContaining({ event_type: 'order.created', topic: 'orders' })
    );
  });

  it('translates payment.approved into an orderUpdated(payment_approved) broadcast', () => {
    const gateway = mockGateway();
    const consumer = new RealtimeConsumer(gateway);

    consumer.handleEvent(JSON.stringify({ type: 'payment.approved', order_id: 'ord_2' }));

    expect(gateway.orderUpdated).toHaveBeenCalledWith(
      expect.objectContaining({ order_id: 'ord_2', status: 'payment_approved' })
    );
    expect(gateway.orderEvent).toHaveBeenCalledWith(
      'ord_2',
      expect.objectContaining({ event_type: 'payment.approved', topic: 'payments' })
    );
  });

  it('translates inventory.reserved into an orderUpdated(inventory_reserved) broadcast', () => {
    const gateway = mockGateway();
    const consumer = new RealtimeConsumer(gateway);

    consumer.handleEvent(JSON.stringify({ type: 'inventory.reserved', order_id: 'ord_3' }));

    expect(gateway.orderUpdated).toHaveBeenCalledWith(
      expect.objectContaining({ order_id: 'ord_3', status: 'inventory_reserved' })
    );
    expect(gateway.orderEvent).toHaveBeenCalledWith(
      'ord_3',
      expect.objectContaining({ event_type: 'inventory.reserved', topic: 'inventory' })
    );
  });

  it('translates notification.sent into an orderUpdated(completed) broadcast', () => {
    const gateway = mockGateway();
    const consumer = new RealtimeConsumer(gateway);

    consumer.handleEvent(JSON.stringify({ type: 'notification.sent', order_id: 'ord_4' }));

    expect(gateway.orderUpdated).toHaveBeenCalledWith(
      expect.objectContaining({ order_id: 'ord_4', status: 'completed' })
    );
    expect(gateway.orderEvent).toHaveBeenCalledWith(
      'ord_4',
      expect.objectContaining({ event_type: 'notification.sent', topic: 'notifications' })
    );
  });

  it('ignores unknown event types and malformed payloads without broadcasting', () => {
    const gateway = mockGateway();
    const consumer = new RealtimeConsumer(gateway);

    consumer.handleEvent(JSON.stringify({ type: 'something.else', order_id: 'ord_5' }));
    consumer.handleEvent('not json');
    consumer.handleEvent(JSON.stringify({ type: 'order.created' })); // missing order_id

    expect(gateway.orderCreated).not.toHaveBeenCalled();
    expect(gateway.orderUpdated).not.toHaveBeenCalled();
    expect(gateway.orderEvent).not.toHaveBeenCalled();
  });
});
