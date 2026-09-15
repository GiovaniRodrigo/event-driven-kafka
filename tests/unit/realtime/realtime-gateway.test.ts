import { SocketRealtimeGateway } from '../../../src/realtime/realtime-gateway';

function mockIo() {
  const roomEmit = jest.fn();
  const io = {
    on: jest.fn(),
    emit: jest.fn(),
    to: jest.fn(() => ({ emit: roomEmit })),
  };
  return { io, roomEmit };
}

describe('SocketRealtimeGateway', () => {
  it('broadcasts order:created globally', () => {
    const { io } = mockIo();
    const gateway = new SocketRealtimeGateway(io as any);
    const order = { order_id: 'ord_1', user_id: 'u1', status: 'pending' as const, total_amount: 10 };

    gateway.orderCreated(order);

    expect(io.emit).toHaveBeenCalledWith('order:created', order);
  });

  it('broadcasts order:updated globally', () => {
    const { io } = mockIo();
    const gateway = new SocketRealtimeGateway(io as any);

    gateway.orderUpdated({ order_id: 'ord_1', status: 'completed' });

    expect(io.emit).toHaveBeenCalledWith('order:updated', { order_id: 'ord_1', status: 'completed' });
  });

  it('emits order:event only to the per-order room, tagged with the order id', () => {
    const { io, roomEmit } = mockIo();
    const gateway = new SocketRealtimeGateway(io as any);

    gateway.orderEvent('ord_1', { event_type: 'payment.approved', topic: 'payments', timestamp: 'T' });

    expect(io.to).toHaveBeenCalledWith('order:ord_1');
    expect(roomEmit).toHaveBeenCalledWith('order:event', {
      order_id: 'ord_1',
      event_type: 'payment.approved',
      topic: 'payments',
      timestamp: 'T',
    });
    // Room-scoped events must not also go out on the global channel.
    expect(io.emit).not.toHaveBeenCalledWith('order:event', expect.anything());
  });

  it('broadcasts consumer:health globally', () => {
    const { io } = mockIo();
    const gateway = new SocketRealtimeGateway(io as any);
    const health = { payment: 'healthy' as const, inventory: 'degraded' as const, notification: 'down' as const };

    gateway.consumerHealth(health);

    expect(io.emit).toHaveBeenCalledWith('consumer:health', health);
  });

  it('lets a socket join/leave its order room on subscribe/unsubscribe', () => {
    const { io } = mockIo();
    // capture the connection handler registered in the constructor
    new SocketRealtimeGateway(io as any);
    const connectionHandler = io.on.mock.calls.find((c) => c[0] === 'connection')?.[1];
    expect(connectionHandler).toBeDefined();

    const handlers: Record<string, (arg: unknown) => void> = {};
    const socket = {
      on: jest.fn((evt: string, fn: (arg: unknown) => void) => {
        handlers[evt] = fn;
      }),
      join: jest.fn(),
      leave: jest.fn(),
    };
    connectionHandler(socket);

    handlers['subscribe']('ord_9');
    expect(socket.join).toHaveBeenCalledWith('order:ord_9');

    handlers['unsubscribe']('ord_9');
    expect(socket.leave).toHaveBeenCalledWith('order:ord_9');

    // ignores non-string ids
    handlers['subscribe'](null);
    expect(socket.join).toHaveBeenCalledTimes(1);
  });
});
