import { ChaosEngine } from '../../src/chaos/chaos-engine';

describe('Chaos Engine Unit Tests', () => {
  let chaos: ChaosEngine;

  beforeEach(() => {
    chaos = ChaosEngine.getInstance();
    chaos.reset();
  });

  it('starts with all chaos faults disabled by default', () => {
    const status = chaos.getStatus();
    expect(status.isChaosActive).toBe(false);
    expect(status.paymentFailure).toBe(false);
    expect(status.inventoryFailure).toBe(false);
  });

  it('toggles fault injection dynamically', () => {
    chaos.setFault({ paymentFailure: true, fraudLatencyMs: 1500 });
    const status = chaos.getStatus();
    expect(status.isChaosActive).toBe(true);
    expect(status.paymentFailure).toBe(true);
    expect(status.fraudLatencyMs).toBe(1500);
    expect(chaos.shouldFail('payment')).toBe(true);
    expect(chaos.shouldFail('inventory')).toBe(false);
  });

  it('resets cleanly back to defaults', () => {
    chaos.setFault({ shippingFailure: true, notificationFailure: true });
    expect(chaos.getStatus().isChaosActive).toBe(true);

    chaos.reset();
    expect(chaos.getStatus().isChaosActive).toBe(false);
  });
});
