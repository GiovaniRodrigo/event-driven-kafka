import { logger } from '../utils/logger';

export interface ChaosConfig {
  paymentFailure: boolean;
  paymentLatencyMs: number;
  inventoryFailure: boolean;
  inventoryLatencyMs: number;
  fraudRejection: boolean;
  fraudLatencyMs: number;
  shippingFailure: boolean;
  notificationFailure: boolean;
  dbFailure: boolean;
  malformedEventRate: number; // 0 to 1
}

const DEFAULT_CHAOS_CONFIG: ChaosConfig = {
  paymentFailure: false,
  paymentLatencyMs: 0,
  inventoryFailure: false,
  inventoryLatencyMs: 0,
  fraudRejection: false,
  fraudLatencyMs: 0,
  shippingFailure: false,
  notificationFailure: false,
  dbFailure: false,
  malformedEventRate: 0,
};

export class ChaosEngine {
  private static instance: ChaosEngine;
  private config: ChaosConfig;

  private constructor() {
    this.config = { ...DEFAULT_CHAOS_CONFIG };
  }

  static getInstance(): ChaosEngine {
    if (!ChaosEngine.instance) {
      ChaosEngine.instance = new ChaosEngine();
    }
    return ChaosEngine.instance;
  }

  getStatus(): ChaosConfig & { isChaosActive: boolean } {
    const isChaosActive = Object.entries(this.config).some(([key, val]) => {
      if (typeof val === 'boolean') return val;
      if (typeof val === 'number') return val > 0;
      return false;
    });
    return { ...this.config, isChaosActive };
  }

  setFault(fault: Partial<ChaosConfig>): void {
    this.config = { ...this.config, ...fault };
    logger.warn({ event: 'chaos_fault_configured', fault, currentConfig: this.config });
  }

  reset(): void {
    this.config = { ...DEFAULT_CHAOS_CONFIG };
    logger.info({ event: 'chaos_reset_to_default' });
  }

  async checkLatency(service: 'payment' | 'inventory' | 'fraud'): Promise<void> {
    let delay = 0;
    if (service === 'payment') delay = this.config.paymentLatencyMs;
    if (service === 'inventory') delay = this.config.inventoryLatencyMs;
    if (service === 'fraud') delay = this.config.fraudLatencyMs;

    if (delay > 0) {
      logger.warn({ event: 'chaos_latency_injected', service, delayMs: delay });
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  shouldFail(service: 'payment' | 'inventory' | 'fraud' | 'shipping' | 'notification' | 'db'): boolean {
    if (service === 'payment') return this.config.paymentFailure;
    if (service === 'inventory') return this.config.inventoryFailure;
    if (service === 'fraud') return this.config.fraudRejection;
    if (service === 'shipping') return this.config.shippingFailure;
    if (service === 'notification') return this.config.notificationFailure;
    if (service === 'db') return this.config.dbFailure;
    return false;
  }
}
