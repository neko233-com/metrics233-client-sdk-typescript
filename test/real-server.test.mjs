import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Metrics233 } from '../dist/index.js';

test('actual metrics233-server accepts SDK data and isolates ingest credentials', { skip: !process.env.METRICS233_TEST_ROOT }, async () => {
  const endpoint = process.env.METRICS233_TEST_ROOT, token = process.env.METRICS233_TEST_INGEST_TOKEN;
  const root = new URL(endpoint); assert.equal(root.protocol, 'http:'); assert.ok(['127.0.0.1', '[::1]', 'localhost'].includes(root.hostname));
  const metrics = new Metrics233({ endpoint, token, allowHttpLoopback: true, labels: { service: 'sdk-contract', implementation: 'typescript' } });
  metrics.counter('sdk_contract_calls_total', 2).gauge('sdk_contract_gauge', 233); await metrics.shutdown();
  const wrong = new Metrics233({ endpoint, token: 'synthetic-wrong-reporting-token-contract', allowHttpLoopback: true }); wrong.gauge('must_not_accept', 1);
  await assert.rejects(wrong.flush(), error => error.status === 401);
  const read = await fetch(new URL('/api/v1/stats', endpoint), { headers: { Authorization: `Bearer ${token}` } }); assert.equal(read.status, 403);
  const manage = await fetch(new URL('/api/v1/targets', endpoint), { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: '{}' }); assert.equal(manage.status, 403);
});
