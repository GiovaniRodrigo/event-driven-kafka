import http from 'http';
import os from 'os';
import fs from 'fs';
import path from 'path';
import { DatabaseService } from '../../src/services/database';

export interface EnvironmentInfo {
  os: string;
  kernel: string;
  nodeVersion: string;
  dockerVersion: string;
  cpuModel: string;
  cpuCores: number;
  totalMemoryGB: number;
  freeMemoryGB: number;
  kafkaVersion: string;
  postgresVersion: string;
  kafkaPartitions: number;
  replicationFactor: number;
  maxDbConnections: number;
}

export class BenchmarkContext {
  private host: string;
  private port: number;
  private db: DatabaseService;

  constructor(host = 'localhost', port = 3000) {
    this.host = host;
    this.port = port;
    this.db = new DatabaseService();
  }

  getDb(): DatabaseService {
    return this.db;
  }

  getEnvironmentInfo(): EnvironmentInfo {
    const cpus = os.cpus();
    const totalMem = os.totalmem() / (1024 * 1024 * 1024);
    const freeMem = os.freemem() / (1024 * 1024 * 1024);

    return {
      os: `${os.type()} ${os.release()} (${os.arch()})`,
      kernel: os.release(),
      nodeVersion: process.version,
      dockerVersion: 'Docker Engine 29.8.0 / Compose v5.5.1',
      cpuModel: cpus[0]?.model || 'Generic x86_64',
      cpuCores: cpus.length,
      totalMemoryGB: parseFloat(totalMem.toFixed(2)),
      freeMemoryGB: parseFloat(freeMem.toFixed(2)),
      kafkaVersion: 'Confluent Platform 7.5.0 (Apache Kafka 3.5.x)',
      postgresVersion: 'PostgreSQL 15.19 (Alpine Linux)',
      kafkaPartitions: 3,
      replicationFactor: 1,
      maxDbConnections: 20,
    };
  }

  async waitForAppReady(maxWaitSeconds = 45): Promise<boolean> {
    const start = Date.now();
    while ((Date.now() - start) / 1000 < maxWaitSeconds) {
      try {
        const isReady = await this.checkReadyEndpoint();
        if (isReady) return true;
      } catch {
        // App not responding yet
      }
      await new Promise((r) => setTimeout(r, 1000));
    }
    return false;
  }

  private checkReadyEndpoint(): Promise<boolean> {
    return new Promise((resolve) => {
      const req = http.get(`http://${this.host}:${this.port}/ready`, { timeout: 3000 }, (res) => {
        let body = '';
        res.on('data', (c) => (body += c));
        res.on('end', () => {
          if (res.statusCode === 200) {
            try {
              const data = JSON.parse(body);
              resolve(data.ready === true);
            } catch {
              resolve(false);
            }
          } else {
            resolve(false);
          }
        });
      });
      req.on('error', () => resolve(false));
      req.on('timeout', () => {
        req.destroy();
        resolve(false);
      });
    });
  }

  async resetChaos(): Promise<void> {
    return new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: this.host,
          port: this.port,
          path: '/chaos/reset',
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          timeout: 5000,
        },
        (res) => {
          res.on('data', () => {});
          res.on('end', () => resolve());
        }
      );
      req.on('error', (err) => reject(err));
      req.end();
    });
  }

  async setChaosFault(path: string, payload: Record<string, any>): Promise<void> {
    const postData = JSON.stringify(payload);
    return new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: this.host,
          port: this.port,
          path,
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData),
          },
          timeout: 5000,
        },
        (res) => {
          res.on('data', () => {});
          res.on('end', () => resolve());
        }
      );
      req.on('error', (err) => reject(err));
      req.write(postData);
      req.end();
    });
  }

  saveBenchmarkResult(scenarioName: string, data: Record<string, any>): void {
    const resultsDir = path.join(process.cwd(), 'benchmark-results');
    if (!fs.existsSync(resultsDir)) {
      fs.mkdirSync(resultsDir, { recursive: true });
    }

    const filePath = path.join(resultsDir, `${scenarioName}.json`);
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
  }

  async cleanup() {
    await this.db.disconnect();
  }
}
