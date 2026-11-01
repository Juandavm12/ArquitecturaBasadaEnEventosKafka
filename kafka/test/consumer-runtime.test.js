import test from 'node:test';
import assert from 'node:assert/strict';
import { createRuntime, readSettings } from '../src/consumer-runtime.js';

function fixture(overrides = {}, dependencies = {}) {
  const listeners = {}, calls = [], records = [];
  let ready = false;
  const consumer = {
    events: { GROUP_JOIN: 'join', CRASH: 'crash', DISCONNECT: 'disconnect' },
    on: (event, listener) => { listeners[event] = listener; },
    connect: async () => { calls.push('connect'); },
    subscribe: async options => { calls.push(options); },
    run: async options => { calls.push(options); },
    stop: async () => { calls.push('stop'); },
    disconnect: async () => { calls.push('disconnect'); },
    commitOffsets: async () => {}, ...overrides,
  };
  const runtime = createRuntime({ consumer, role: 'inventory', instanceId: 'host',
    logger: record => records.push(record), marker: value => { ready = value; },
    onFatal: code => calls.push(['exit', code]),
    deadline: () => () => {}, ...dependencies,
  });
  return { runtime, consumer, listeners, calls, records, ready: () => ready };
}

test('settings validate role and bounded integer delay', () => {
  assert.equal(readSettings({ CONSUMER_ROLE: 'billing' }).delayMs, 0);
  assert.equal(readSettings({ CONSUMER_ROLE: 'inventory', PROCESSING_DELAY_MS: '10000' }).delayMs, 10000);
  for (const value of ['-1', '10001', '1.2', '', 'NaN']) {
    assert.throws(() => readSettings({ CONSUMER_ROLE: 'billing', PROCESSING_DELAY_MS: value }));
  }
  assert.throws(() => readSettings({ CONSUMER_ROLE: 'other' }));
});

test('listeners precede run; only group join marks ready; disconnect clears', async () => {
  const f = fixture({ run: async options => {
    assert.equal(typeof f.listeners.join, 'function');
    assert.equal(options.autoCommit, false);
    assert.equal(typeof options.eachMessage, 'function');
  } });
  await f.runtime.start();
  assert.equal(f.ready(), false);
  assert.deepEqual(f.calls[1], { topic: 'orders.events', fromBeginning: true });
  f.listeners.join({ payload: { memberAssignment: { 'orders.events': [0] } } });
  assert.equal(f.ready(), true);
  assert.equal(f.records.at(-1).groupId, 'inventory-service-group');
  f.listeners.disconnect();
  assert.equal(f.ready(), false);
  await f.runtime.shutdown();
  f.listeners.join({ payload: {} });
  assert.equal(f.ready(), false);
});

test('crash even when run resolves clears marker and exits via cleanup', async () => {
  const f = fixture({ run: async () => {
    f.listeners.join({ payload: {} });
    f.listeners.crash({ payload: { error: new Error('broker failed') } });
  } });
  await f.runtime.start();
  await f.runtime.shutdown();
  assert.equal(f.ready(), false);
  assert.ok(f.records.some(record => record.phase === 'fatal'));
  assert.deepEqual(f.calls.at(-1), ['exit', 1]);
});

test('bootstrap errors propagate after fatal cleanup', async () => {
  const f = fixture({ connect: async () => { throw new Error('connect failed'); } });
  await assert.rejects(f.runtime.start(), /connect failed/);
  assert.equal(f.ready(), false);
  assert.deepEqual(f.calls.at(-1), ['exit', 1]);
});

test('shutdown is idempotent and waits for stop then disconnect', async () => {
  let release;
  const pending = new Promise(resolve => { release = resolve; });
  const f = fixture({ stop: () => pending });
  await f.runtime.start();
  const first = f.runtime.shutdown();
  assert.equal(first, f.runtime.shutdown());
  assert.ok(!f.calls.includes('disconnect'));
  release();
  await first;
  assert.ok(f.calls.includes('disconnect'));
});

test('shutdown during connect waits, then disconnects without starting run', async () => {
  let release;
  const pending = new Promise(resolve => { release = resolve; });
  const f = fixture({ connect: () => pending });
  const starting = f.runtime.start();
  const closing = f.runtime.shutdown();
  await Promise.resolve();
  assert.ok(!f.calls.includes('disconnect'));
  release();
  await Promise.all([starting, closing]);
  assert.deepEqual(f.calls, ['stop', 'disconnect']);
});

test('stop failure still disconnects, rejects and takes fatal exit path', async () => {
  const f = fixture({ stop: async () => { throw new Error('stop failed'); } });
  await assert.rejects(f.runtime.shutdown(), /stop failed/);
  assert.ok(f.calls.includes('disconnect'));
  assert.deepEqual(f.calls.at(-1), ['exit', 1]);
});

test('deadline forces fatal exit while cleanup is pending, without real timers', async () => {
  let expire, release;
  const pending = new Promise(resolve => { release = resolve; });
  const f = fixture({ stop: () => pending }, {
    deadline: callback => { expire = callback; return () => {}; },
  });
  const closing = f.runtime.shutdown();
  await Promise.resolve();
  expire();
  assert.deepEqual(f.calls.at(-1), ['exit', 1]);
  assert.equal(f.records.at(-1).phase, 'shutdown-timeout');
  release();
  await closing;
});

for (const stage of ['subscribe', 'run']) {
  test(`${stage} bootstrap failure cleans up and rejects`, async () => {
    const f = fixture({ [stage]: async () => { throw new Error(stage); } });
    await assert.rejects(f.runtime.start(), new RegExp(stage));
    assert.equal(f.ready(), false);
    assert.ok(f.calls.includes('disconnect'));
    assert.deepEqual(f.calls.at(-1), ['exit', 1]);
  });
}

test('disconnect failure rejects and takes fatal exit path', async () => {
  const f = fixture({ disconnect: async () => { throw new Error('disconnect failed'); } });
  await assert.rejects(f.runtime.shutdown(), /disconnect failed/);
  assert.deepEqual(f.calls.at(-1), ['exit', 1]);
});
