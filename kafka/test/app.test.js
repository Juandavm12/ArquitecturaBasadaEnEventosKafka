import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createApp } from '../src/app.js';

const input = { customerName: 'Ana', product: 'Libro', quantity: 2, unitPrice: 10 };
async function fixture(t, overrides = {}) {
  const calls = [], logs = [];
  const producer = { send: async payload => {
    calls.push(payload);
    return [{ partition: 1, baseOffset: '7' }];
  } };
  const server = createApp({ producer, topic: 'orders.events',
    checkBroker: async () => {}, log: entry => logs.push(entry), ...overrides }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => {
    server.close(resolve);
    server.closeAllConnections();
  }));
  const url = `http://127.0.0.1:${server.address().port}`;
  const post = (body = JSON.stringify(input)) => fetch(`${url}/orders`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body,
  });
  return { calls, logs, post, url };
}
test('202 confirma publicación y conserva contrato, headers y metadata', async t => {
  const f = await fixture(t);
  const response = await f.post();
  assert.equal(response.status, 202);
  const result = await response.json();
  const sent = f.calls[0], message = sent.messages[0];
  const event = JSON.parse(message.value);
  assert.deepEqual(result, { status: 'accepted',
    message: 'Pedido recibido y evento OrderCreated publicado en Kafka', orderId: event.data.orderId });
  assert.equal(sent.topic, 'orders.events');
  assert.equal(sent.acks, -1);
  assert.equal(sent.timeout, 10000);
  assert.equal(message.key, event.data.orderId);
  assert.deepEqual(message.headers, { correlationId: event.correlationId, eventId: event.eventId });
  assert.equal(event.eventType, 'OrderCreated');
  assert.equal(event.data.total, 20);
  assert.equal(event.partition, undefined);
  assert.equal(f.logs[0].partition, 1);
  assert.equal(f.logs[0].baseOffset, '7');
  for (const key of ['date', 'service', 'hostname', 'orderId', 'eventId', 'correlationId', 'topic']) {
    assert.ok(f.logs[0][key]);
  }
});
for (const [name, body, status] of [
  ['inválido', '{}', 400], ['JSON roto', '{', 400], ['grande', JSON.stringify({ x: 'a'.repeat(33000) }), 413],
]) test(`${name}: no publica`, async t => {
  const f = await fixture(t);
  assert.equal((await f.post(body)).status, status);
  assert.equal(f.calls.length, 0);
});
test('fallo broker: 503 sin accepted ni stack; conserva IDs internos', async t => {
  const f = await fixture(t, { producer: { send: async () => { throw new Error('private broker'); } } });
  const response = await f.post();
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { error: 'Kafka no disponible' });
  assert.ok(f.logs[0].eventId);
  assert.equal(f.logs[0].error, 'private broker');
});
for (const healthy of [true, false]) test(`health ${healthy} sin publicar`, async t => {
  const f = await fixture(t, { checkBroker: async () => {
    if (!healthy) throw new Error('offline');
  } });
  assert.equal((await fetch(`${f.url}/health`)).status, healthy ? 200 : 503);
  assert.equal(f.calls.length, 0);
});
test('espera send iniciado antes de responder', { timeout: 3000 }, async t => {
  let release, entered;
  const started = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const f = await fixture(t, { producer: { send: async () => {
    entered();
    await gate;
    return [];
  } } });
  let responded = false;
  const pending = f.post().then(response => { responded = true; return response; });
  try {
    await started;
    await new Promise(resolve => setTimeout(resolve, 40));
    assert.equal(responded, false);
  } finally { release(); }
  assert.equal((await pending).status, 202);
});
