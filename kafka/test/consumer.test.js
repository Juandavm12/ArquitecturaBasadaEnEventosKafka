import test from 'node:test';
import assert from 'node:assert/strict';
import { getConfig, createSimulatedAction, createMessageHandler } from '../src/consumer.js';

const event = () => ({
  eventId: 'evt-1', eventType: 'OrderCreated', eventVersion: '1.0',
  occurredAt: '2026-01-01T00:00:00.000Z', source: 'orders-api',
  correlationId: 'corr-1', data: { orderId: 'ORD-1' },
});
function setup(options = {}) {
  const calls = [];
  const records = [];
  const payload = {
    topic: 'orders', partition: 0,
    message: { offset: '42', value: Buffer.from(JSON.stringify(event())) },
    heartbeat: async () => { calls.push('heartbeat'); },
  };
  const handler = createMessageHandler({
    role: 'inventory', instanceId: 'instance-1',
    action: async () => { calls.push('action'); },
    logger: (record) => records.push(record),
    commitOffsets: async (offsets) => { calls.push(offsets); },
    ...options,
  });
  return { calls, records, payload, handler };
}

test('tres roles tienen grupos, servicios y acciones semánticas distintos', async () => {
  const expected = {
    inventory: ['inventory-service-group', 'inventory-consumer', 'simulate-stock-reservation'],
    billing: ['billing-service-group', 'billing-consumer', 'simulate-invoice-charge'],
    notification: ['notification-service-group', 'notification-consumer', 'simulate-notification-send'],
  };
  for (const [role, [groupId, service, action]] of Object.entries(expected)) {
    assert.deepEqual(getConfig(role), { groupId, service, action });
    assert.deepEqual(await createSimulatedAction({ role })(event()), { action, orderId: 'ORD-1' });
  }
  assert.throws(() => getConfig('unknown'), /role/);
});

test('registra evento y trazas; heartbeat rodea acción y commit es último', async () => {
  const { handler, payload, calls, records } = setup();
  await handler(payload);
  assert.deepEqual(calls, ['heartbeat', 'action', 'heartbeat',
    [{ topic: 'orders', partition: 0, offset: '43' }]]);
  assert.deepEqual(records.map((r) => r.phase), ['received', 'processed', 'offset-committed']);
  assert.deepEqual(records[0].event, event());
  for (const record of records) {
    assert.deepEqual(record.trace, {
      service: 'inventory-consumer', groupId: 'inventory-service-group',
      instanceId: 'instance-1', orderId: 'ORD-1', eventId: 'evt-1',
      correlationId: 'corr-1', topic: 'orders', partition: 0, offset: '42',
    });
    assert.equal(record.action, 'simulate-stock-reservation');
  }
});

test('no resuelve ni confirma mientras acción está pendiente', async () => {
  let release;
  let entered;
  const gate = new Promise((resolve) => { release = resolve; });
  const started = new Promise((resolve) => { entered = resolve; });
  const { handler, payload, calls } = setup({ action: async () => { entered(); await gate; } });
  let settled = false;
  const running = handler(payload).then(() => { settled = true; });
  await started;
  assert.equal(settled, false);
  assert.deepEqual(calls, ['heartbeat']);
  release();
  await running;
  assert.equal(settled, true);
  assert.equal(calls.at(-1)[0].offset, '43');
});

test('error de acción se propaga sin commit', async () => {
  const error = new Error('action failed');
  const { handler, payload, calls } = setup({ action: async () => { throw error; } });
  await assert.rejects(handler(payload), (e) => e === error);
  assert.deepEqual(calls, ['heartbeat']);
});

test('JSON, tipo, versión, IDs y metadata inválidos no actúan ni confirman', async () => {
  const invalid = ['{', ...[
    { eventType: 'Other' }, { eventVersion: '2.0' }, { eventId: '' },
    { correlationId: ' ' }, { data: {} }, { source: '' }, { occurredAt: 'bad' },
  ].map((patch) => JSON.stringify({ ...event(), ...patch }))];
  for (const value of invalid) {
    const { handler, payload, calls } = setup();
    payload.message.value = Buffer.from(value);
    await assert.rejects(handler(payload));
    assert.deepEqual(calls, []);
  }
});

test('offset grande no pierde precisión', async () => {
  const { handler, payload, calls } = setup();
  payload.message.offset = '9007199254740993';
  await handler(payload);
  assert.equal(calls.at(-1)[0].offset, '9007199254740994');
});

test('offset inválido se rechaza antes de acción', async () => {
  for (const offset of ['-1', '1.2', 'abc', '', 42]) {
    const { handler, payload, calls } = setup();
    payload.message.offset = offset;
    await assert.rejects(handler(payload), /offset/);
    assert.deepEqual(calls, []);
  }
});

test('error de commit se propaga sin registrar offset-committed', async () => {
  const error = new Error('commit failed');
  const { handler, payload, records } = setup({ commitOffsets: async () => { throw error; } });
  await assert.rejects(handler(payload), (e) => e === error);
  assert.deepEqual(records.map((r) => r.phase), ['received', 'processed']);
});

test('fallo de heartbeat pre/post impide commit y propaga error', async () => {
  for (const failAt of [1, 2]) {
    const { handler, payload, calls } = setup();
    let count = 0;
    const error = new Error('heartbeat failed');
    payload.heartbeat = async () => { if (++count === failAt) throw error; };
    await assert.rejects(handler(payload), (e) => e === error);
    assert.deepEqual(calls, failAt === 1 ? [] : ['action']);
  }
});

test('no modifica bytes ni evento recibido', async () => {
  const { handler, payload, records } = setup();
  const before = Buffer.from(payload.message.value);
  await handler(payload);
  assert.deepEqual(payload.message.value, before);
  assert.deepEqual(records[0].event, event());
});

test('delay opt-in usa sleep inyectado; rechaza valores inválidos', async () => {
  const sleeps = [];
  const sleep = async (ms) => { sleeps.push(ms); };
  await createSimulatedAction({ role: 'billing', sleep })(event());
  await createSimulatedAction({ role: 'billing', delayMs: 25, sleep })(event());
  assert.deepEqual(sleeps, [25]);
  for (const delayMs of [-1, NaN, Infinity, '25']) {
    assert.throws(() => createSimulatedAction({ role: 'billing', delayMs, sleep }), /delayMs/);
  }
});
