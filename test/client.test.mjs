import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { Metrics233, MetricsError } from '../dist/index.js';
const token = 'synthetic-reporting-token-contract-32-chars';
async function fixture(run, handler) {
  const received = [];
  const server = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const body = Buffer.concat(chunks).toString(); received.push({ body, authorization: req.headers.authorization, url: req.url });
    if (handler) return handler(req, res, received);
    const samples = JSON.parse(body).samples; res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ accepted: samples.length }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try { await run(`http://127.0.0.1:${server.address().port}`, received); } finally { await new Promise(resolve => server.close(resolve)); }
}
test('constructor is inert; fluent views share cumulative counters and exact wire authentication', async () => {
  await fixture(async (endpoint, received) => {
    const m = new Metrics233({ endpoint: `${endpoint}/prefix`, token, allowHttpLoopback: true, batchSize: 1, labels: { serverId: '1' } });
    const v = m.withLabels({ environment: 'test' }); assert.equal(received.length, 0);
    v.counter('calls_total', 2).gauge('online', 0); await m.flush();
    v.counter('calls_total', 3); await m.shutdown();
    const samples = received.flatMap(x => JSON.parse(x.body).samples);
    assert.deepEqual(samples.filter(x => x.name === 'calls_total').map(x => x.value), [2, 5]);
    assert.equal(samples.find(x => x.name === 'online').value, 0);
    assert.equal(samples[0].labels.serverId, '1'); assert.equal(samples[0].labels.environment, 'test');
    for (const value of received) { assert.equal(value.authorization, `Bearer ${token}`); assert.equal(value.url, '/prefix/api/v1/ingest'); }
    assert.throws(() => v.gauge('after_close', 1), /closed/);
    assert.equal(JSON.stringify(m).includes(token), false);
  });
});
test('failed and partial ACK batches preserve exact timestamps and bodies; new updates survive shutdown', async () => {
  await fixture(async (endpoint, received) => {
    const m = new Metrics233({ endpoint, token, allowHttpLoopback: true, retries: 0 });
    m.counter('calls_total', 1); await assert.rejects(m.flush(), error => error.code === 'partial_ack');
    m.counter('calls_total', 2); await m.shutdown();
    assert.equal(received[0].body, received[1].body);
    assert.equal(JSON.parse(received[2].body).samples[0].value, 3);
  }, (_req, res, received) => { res.end(JSON.stringify({ accepted: received.length === 1 ? 0 : JSON.parse(received.at(-1).body).samples.length })); });
});
test('bounded raw queue, kind conflicts and invalid values fail before mutation', async () => {
  await fixture(async endpoint => {
    const m = new Metrics233({ endpoint, token, allowHttpLoopback: true, maxSeries: 2, maxPending: 1 });
    m.record({ name: 'raw', value: 1 }); assert.throws(() => m.record({ name: 'raw', value: 2 }), /pending_limit/);
    assert.throws(() => m.gauge('raw', 1), /metric_kind_conflict/);
    assert.throws(() => m.record({ name: 'invalid', value: 1, timestamp: NaN }), /invalid_timestamp/);
    m.gauge('other', 1); assert.throws(() => m.gauge('third', 1), /series_limit/);
    assert.throws(() => m.counter('other', -1), /invalid_sample/); await m.shutdown();
  });
});
test('wrong reporting credential is not retried and cannot be disclosed in errors', async () => {
  await fixture(async (endpoint, received) => {
    const m = new Metrics233({ endpoint, token, allowHttpLoopback: true }); m.gauge('value', 1);
    await assert.rejects(m.flush(), error => error instanceof MetricsError && error.status === 401 && !String(error).includes(token));
    assert.equal(received.length, 1); assert.equal(m.stats().pendingBatches, 1);
  }, (_req, res) => { res.statusCode = 401; res.end('private body must not appear in errors'); });
});
test('HTTPS policy rejects remote cleartext and embedded URL credentials', () => {
  for (const endpoint of ['http://monitoring.example.com', 'https://user:pass@monitoring.example.com', 'https://monitoring.example.com?token=secret']) assert.throws(() => new Metrics233({ endpoint, token }), /invalid_endpoint/);
});

test('redirects are rejected once and never forward the reporting credential', async () => {
  await fixture(async (destination, forwarded) => {
    await fixture(async (endpoint, received) => {
      const m = new Metrics233({ endpoint, token, allowHttpLoopback: true }); m.gauge('value', 1);
      await assert.rejects(m.flush(), error => error.status === 302); assert.equal(received.length, 1); assert.equal(forwarded.length, 0);
    }, (_req, res) => { res.statusCode = 302; res.setHeader('location', destination); res.end(); });
  });
});
test('Retry-After persists across calls without bypassing the total deadline', async () => {
  await fixture(async (endpoint, received) => {
    const m = new Metrics233({ endpoint, token, allowHttpLoopback: true, timeoutMs: 100 }); m.gauge('value', 1);
    await assert.rejects(m.flush(), error => error.code === 'deadline');
    await assert.rejects(m.flush(), error => error.code === 'retry_after'); assert.equal(received.length, 1);
  }, (_req, res) => { res.statusCode = 429; res.setHeader('retry-after', '120'); res.end(); });
});
test('oversized acknowledgement does not dequeue the batch', async () => {
  await fixture(async endpoint => {
    const m = new Metrics233({ endpoint, token, allowHttpLoopback: true }); m.gauge('value', 1);
    await assert.rejects(m.flush(), error => error.code === 'invalid_ack'); assert.equal(m.stats().pendingBatches, 1);
  }, (_req, res) => res.end('x'.repeat(65537)));
});
