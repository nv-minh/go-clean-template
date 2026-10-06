// k6 load test. Run: make load-test   (BASE_URL, RATE, DURATION are overridable)
//   k6 run -e BASE_URL=http://localhost:8080 -e RATE=500 -e DURATION=2m test/load/orders.js
import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '300');
const DURATION = __ENV.DURATION || '1m';
const HOT_ORDERS = 50;
// Defaults suit a laptop where k6, the app and every dependency share the same CPUs.
// Tighten them for a dedicated staging environment: -e READ_P95_MS=50 -e WRITE_P95_MS=150
const READ_P95 = __ENV.READ_P95_MS || '150';
const READ_P99 = __ENV.READ_P99_MS || '500';
const WRITE_P95 = __ENV.WRITE_P95_MS || '300';
const WRITE_P99 = __ENV.WRITE_P99_MS || '1000';

export const options = {
  scenarios: {
    // Constant arrival rate (open model): does not slow down when the server slows down,
    // which is what real users do. A closed model hides overload.
    mixed: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 2000,
      stages: [
        { target: RATE, duration: '20s' },
        { target: RATE, duration: DURATION },
        { target: 0, duration: '10s' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{kind:read}': [`p(95)<${READ_P95}`, `p(99)<${READ_P99}`],
    'http_req_duration{kind:write}': [`p(95)<${WRITE_P95}`, `p(99)<${WRITE_P99}`],
  },
};

const customer = uuidv4();
const headers = { 'Content-Type': 'application/json' };

function createOrder(extra = {}) {
  const body = JSON.stringify({
    customer_id: customer,
    currency: 'USD',
    items: [{ sku: 'SKU-' + Math.floor(Math.random() * 1000), quantity: 1 + Math.floor(Math.random() * 3), unit_price: 1999 }],
  });
  return http.post(`${BASE}/v1/orders`, body, { headers: { ...headers, ...extra }, tags: { kind: 'write' } });
}

export function setup() {
  const ids = [];
  for (let i = 0; i < HOT_ORDERS; i++) {
    ids.push(createOrder().json('id'));
  }
  return { ids };
}

export default function (data) {
  // 90% reads on a small hot set (cache friendly, like real traffic), 10% writes.
  if (Math.random() < 0.9) {
    const id = data.ids[Math.floor(Math.random() * data.ids.length)];
    const res = http.get(`${BASE}/v1/orders/${id}`, { tags: { kind: 'read' } });
    check(res, { 'read 200': (r) => r.status === 200 });
  } else {
    const res = createOrder({ 'Idempotency-Key': uuidv4() });
    check(res, { 'create 201': (r) => r.status === 201 });
  }
}
