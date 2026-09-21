import http from 'http';

interface LoadStats {
  totalSent: number;
  success: number;
  failed: number;
  latencies: number[];
  startTime: number;
  endTime: number;
}

const API_HOST = process.env.API_HOST || 'localhost';
const API_PORT = Number(process.env.API_PORT) || 3000;
const TOTAL_ORDERS = Number(process.env.TOTAL_ORDERS) || 100;
const CONCURRENCY = Number(process.env.CONCURRENCY) || 10;

async function sendOrder(orderIdx: number): Promise<number> {
  const start = Date.now();
  const payload = JSON.stringify({
    user_id: `user_load_${orderIdx % 20}`,
    items: [
      {
        sku: 'LAPTOP-001',
        name: 'Workstation',
        price: 99.99,
        quantity: 1,
      },
    ],
  });

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: API_HOST,
        port: API_PORT,
        path: '/orders',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
        },
      },
      (res) => {
        let body = '';
        res.on('data', (chunk) => (body += chunk));
        res.on('end', () => {
          if (res.statusCode === 202) {
            resolve(Date.now() - start);
          } else {
            reject(new Error(`Status ${res.statusCode}: ${body}`));
          }
        });
      }
    );

    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

async function runLoadTest() {
  console.log(`=======================================================`);
  console.log(` Starting Event-Driven Kafka Load Benchmark`);
  console.log(` Target: http://${API_HOST}:${API_PORT}/orders`);
  console.log(` Total Orders: ${TOTAL_ORDERS} | Concurrency: ${CONCURRENCY}`);
  console.log(`=======================================================\n`);

  const stats: LoadStats = {
    totalSent: 0,
    success: 0,
    failed: 0,
    latencies: [],
    startTime: Date.now(),
    endTime: 0,
  };

  let currentIndex = 0;

  async function worker() {
    while (currentIndex < TOTAL_ORDERS) {
      const idx = ++currentIndex;
      try {
        const latency = await sendOrder(idx);
        stats.success++;
        stats.latencies.push(latency);
      } catch (err) {
        stats.failed++;
      }
      stats.totalSent++;
    }
  }

  const workers = Array.from({ length: CONCURRENCY }, () => worker());
  await Promise.all(workers);
  stats.endTime = Date.now();

  const totalTimeSec = (stats.endTime - stats.startTime) / 1000;
  const throughput = (stats.success / totalTimeSec).toFixed(2);

  stats.latencies.sort((a, b) => a - b);
  const p50 = stats.latencies[Math.floor(stats.latencies.length * 0.5)] || 0;
  const p95 = stats.latencies[Math.floor(stats.latencies.length * 0.95)] || 0;
  const p99 = stats.latencies[Math.floor(stats.latencies.length * 0.99)] || 0;

  console.log(`=======================================================`);
  console.log(` Load Test Execution Complete`);
  console.log(`-------------------------------------------------------`);
  console.log(` Total Orders Submitted: ${stats.totalSent}`);
  console.log(` Successful (202 Accepted): ${stats.success}`);
  console.log(` Failed: ${stats.failed}`);
  console.log(` Total Duration: ${totalTimeSec.toFixed(2)}s`);
  console.log(` Ingestion Throughput: ${throughput} orders/sec`);
  console.log(` Latency p50: ${p50}ms`);
  console.log(` Latency p95: ${p95}ms`);
  console.log(` Latency p99: ${p99}ms`);
  console.log(`=======================================================\n`);
}

if (require.main === module) {
  runLoadTest().catch((err) => {
    console.error('Load test failed to start:', err.message);
    process.exit(1);
  });
}
