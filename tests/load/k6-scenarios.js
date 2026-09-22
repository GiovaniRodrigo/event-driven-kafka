import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    baseline: {
      executor: 'constant-arrival-rate',
      rate: 10,
      timeUnit: '1s',
      duration: '1m',
      preAllocatedVUs: 10,
      maxVUs: 50,
      exec: 'baselineScenario',
    },
    normal_load: {
      executor: 'constant-arrival-rate',
      rate: 35,
      timeUnit: '1s',
      duration: '1m',
      preAllocatedVUs: 20,
      maxVUs: 100,
      startTime: '1m10s',
      exec: 'normalScenario',
    },
    spike_load: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 30,
      maxVUs: 200,
      startTime: '2m20s',
      stages: [
        { target: 10, duration: '15s' },
        { target: 150, duration: '15s' },
        { target: 10, duration: '15s' },
      ],
      exec: 'spikeScenario',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<150'],
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:3000';

function postOrder(prefix, index) {
  const payload = JSON.stringify({
    user_id: `${prefix}_${index % 50}`,
    items: [
      {
        sku: 'LAPTOP-001',
        name: 'High Performance Workstation',
        price: 199.99,
        quantity: 1,
      },
    ],
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    timeout: '5s',
  };

  const res = http.post(`${BASE_URL}/orders`, payload, params);

  check(res, {
    'status is 202 Accepted': (r) => r.status === 202,
    'has order_id': (r) => {
      try {
        const json = JSON.parse(r.body);
        return typeof json.order_id === 'string' && json.order_id.length > 0;
      } catch {
        return false;
      }
    },
  });
}

export function baselineScenario() {
  postOrder('k6_baseline', __ITER);
}

export function normalScenario() {
  postOrder('k6_normal', __ITER);
}

export function spikeScenario() {
  postOrder('k6_spike', __ITER);
}
