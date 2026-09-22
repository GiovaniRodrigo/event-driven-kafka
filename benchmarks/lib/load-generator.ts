import http from 'http';
import { LatencyStats } from './metrics-collector';

export interface LoadGeneratorOptions {
  host?: string;
  port?: number;
  ratePerSec: number;
  durationSeconds: number;
  concurrency?: number;
  userIdPrefix?: string;
  customPayloadGenerator?: (idx: number) => Record<string, any>;
}

export interface StepProfile {
  targetRate: number;
  durationSeconds: number;
}

export interface LoadResult {
  totalRequests: number;
  successfulRequests: number;
  failedRequests: number;
  errorRatePercent: number;
  actualDurationSeconds: number;
  actualThroughput: number;
  latencyStats: LatencyStats;
  statusCodes: { [code: number]: number };
  errorDetails: { [message: string]: number };
}

export class LoadGenerator {
  private host: string;
  private port: number;
  private agent: http.Agent;

  constructor(host = 'localhost', port = 3000) {
    this.host = host;
    this.port = port;
    this.agent = new http.Agent({
      keepAlive: true,
      maxSockets: 200,
      maxFreeSockets: 50,
      timeout: 30000,
    });
  }

  async runConstantLoad(options: LoadGeneratorOptions): Promise<LoadResult> {
    const {
      ratePerSec,
      durationSeconds,
      userIdPrefix = 'user_bench',
      customPayloadGenerator,
    } = options;

    const totalTargetRequests = Math.floor(ratePerSec * durationSeconds);
    const intervalMs = 1000 / ratePerSec;
    const latencies: number[] = [];
    const statusCodes: { [code: number]: number } = {};
    const errorDetails: { [message: string]: number } = {};

    let totalSent = 0;
    let successful = 0;
    let failed = 0;

    const startTime = Date.now();
    const activePromises: Promise<void>[] = [];

    for (let i = 0; i < totalTargetRequests; i++) {
      const scheduledTime = startTime + i * intervalMs;
      const now = Date.now();
      const delay = Math.max(0, scheduledTime - now);

      if (delay > 0) {
        await new Promise((r) => setTimeout(r, delay));
      }

      const orderIndex = i + 1;
      const payload = customPayloadGenerator
        ? customPayloadGenerator(orderIndex)
        : {
            user_id: `${userIdPrefix}_${orderIndex % 50}`,
            items: [
              {
                sku: 'LAPTOP-001',
                name: 'High Performance Workstation',
                price: 199.99,
                quantity: 1,
              },
            ],
          };

      const reqPromise = this.sendOrderRequest(payload)
        .then(({ statusCode, durationMs }) => {
          totalSent++;
          statusCodes[statusCode] = (statusCodes[statusCode] || 0) + 1;
          latencies.push(durationMs);
          if (statusCode === 202) {
            successful++;
          } else {
            failed++;
          }
        })
        .catch((err: Error) => {
          totalSent++;
          failed++;
          const msg = err.message || 'Unknown network error';
          errorDetails[msg] = (errorDetails[msg] || 0) + 1;
        });

      activePromises.push(reqPromise);
    }

    await Promise.all(activePromises);
    const endTime = Date.now();
    const actualDurationSeconds = parseFloat(((endTime - startTime) / 1000).toFixed(2));
    const actualThroughput = parseFloat((successful / actualDurationSeconds).toFixed(2));
    const errorRatePercent = totalSent > 0 ? parseFloat(((failed / totalSent) * 100).toFixed(2)) : 0;

    const latencyStats = this.computeStats(latencies);

    return {
      totalRequests: totalSent,
      successfulRequests: successful,
      failedRequests: failed,
      errorRatePercent,
      actualDurationSeconds,
      actualThroughput,
      latencyStats,
      statusCodes,
      errorDetails,
    };
  }

  async runSteppedLoad(steps: StepProfile[], userIdPrefix = 'user_stress'): Promise<{
    stepResults: { stepRate: number; result: LoadResult }[];
    overallResult: LoadResult;
  }> {
    const stepResults: { stepRate: number; result: LoadResult }[] = [];
    const allLatencies: number[] = [];
    let totalSent = 0;
    let totalSuccess = 0;
    let totalFailed = 0;
    const combinedStatusCodes: { [code: number]: number } = {};
    const combinedErrors: { [message: string]: number } = {};
    const startTime = Date.now();

    for (const step of steps) {
      const res = await this.runConstantLoad({
        ratePerSec: step.targetRate,
        durationSeconds: step.durationSeconds,
        userIdPrefix,
      });

      stepResults.push({ stepRate: step.targetRate, result: res });
      totalSent += res.totalRequests;
      totalSuccess += res.successfulRequests;
      totalFailed += res.failedRequests;

      for (const [code, count] of Object.entries(res.statusCodes)) {
        combinedStatusCodes[Number(code)] = (combinedStatusCodes[Number(code)] || 0) + count;
      }
      for (const [err, count] of Object.entries(res.errorDetails)) {
        combinedErrors[err] = (combinedErrors[err] || 0) + count;
      }
    }

    const endTime = Date.now();
    const actualDurationSeconds = parseFloat(((endTime - startTime) / 1000).toFixed(2));
    const actualThroughput = parseFloat((totalSuccess / actualDurationSeconds).toFixed(2));
    const errorRatePercent = totalSent > 0 ? parseFloat(((totalFailed / totalSent) * 100).toFixed(2)) : 0;

    const overallResult: LoadResult = {
      totalRequests: totalSent,
      successfulRequests: totalSuccess,
      failedRequests: totalFailed,
      errorRatePercent,
      actualDurationSeconds,
      actualThroughput,
      latencyStats: this.computeStats(allLatencies),
      statusCodes: combinedStatusCodes,
      errorDetails: combinedErrors,
    };

    return { stepResults, overallResult };
  }

  private sendOrderRequest(payload: Record<string, any>): Promise<{ statusCode: number; durationMs: number }> {
    const postData = JSON.stringify(payload);
    const start = Date.now();

    return new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: this.host,
          port: this.port,
          path: '/orders',
          method: 'POST',
          agent: this.agent,
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData),
          },
          timeout: 10000,
        },
        (res) => {
          let body = '';
          res.on('data', (c) => (body += c));
          res.on('end', () => {
            const durationMs = Date.now() - start;
            resolve({ statusCode: res.statusCode || 500, durationMs });
          });
        }
      );

      req.on('error', (err) => reject(err));
      req.on('timeout', () => {
        req.destroy(new Error('HTTP request timed out after 10000ms'));
      });
      req.write(postData);
      req.end();
    });
  }

  private computeStats(durations: number[]): LatencyStats {
    if (durations.length === 0) {
      return { count: 0, p50: 0, p95: 0, p99: 0, min: 0, max: 0, avg: 0 };
    }

    const sorted = [...durations].sort((a, b) => a - b);
    const count = sorted.length;
    const p50 = sorted[Math.floor(count * 0.5)];
    const p95 = sorted[Math.floor(count * 0.95)];
    const p99 = sorted[Math.floor(count * 0.99)];
    const min = sorted[0];
    const max = sorted[count - 1];
    const sum = sorted.reduce((acc, v) => acc + v, 0);
    const avg = parseFloat((sum / count).toFixed(2));

    return { count, p50, p95, p99, min, max, avg };
  }

  close() {
    this.agent.destroy();
  }
}
