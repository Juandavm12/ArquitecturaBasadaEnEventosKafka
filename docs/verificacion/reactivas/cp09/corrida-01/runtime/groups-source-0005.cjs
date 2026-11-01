'use strict';
// Administrative diagnostic only; never CP09 execution evidence.
const assert = require('node:assert/strict');
const GROUPS = Object.freeze(['inventory', 'billing', 'notification'].map(r => r + '-service-group'));
const TOPIC = 'orders.events';
function config(brokers) {
  assert.ok(Array.isArray(brokers) && brokers.length && brokers.every(b => /^[a-zA-Z0-9.-]+:\d+$/.test(b)));
  return { brokers, clientId: 'cp09-readonly-diagnostic', connectionTimeout: 1000, requestTimeout: 2000, enforceRequestTimeout: true, retry: { retries: 0 }, logCreator: () => ({ level, log }) => process.stderr.write(JSON.stringify({ level, message: log.message }) + '\n') };
}
function numeric(value) {
  assert.equal(typeof value, 'string'); assert.match(value, /^(0|[1-9]\d*)$/);
  const n = Number(value); assert.ok(Number.isSafeInteger(n)); return n;
}
function partitions(rows) {
  assert.ok(Array.isArray(rows)); assert.deepEqual(rows.map(p => p.partition).sort(), [0, 1]); return rows;
}
function errorInfo(e) { return { name: e?.name || 'Error', message: String(e?.message || e) }; }
function bounded(fn, ms, phase) {
  let timer;
  return Promise.race([
    Promise.resolve().then(fn),
    new Promise((_, reject) => { timer = setTimeout(() => { const e = new Error('Deadline exceeded: ' + phase); e.name = 'TimeoutError'; reject(e); }, Math.max(0, ms)); }),
  ]).finally(() => clearTimeout(timer));
}
async function probe({ admin, decode, operationBudgetMs = 15000, cleanupBudgetMs = 2000 }) {
  assert.ok(Number.isInteger(operationBudgetMs) && operationBudgetMs > 0 && operationBudgetMs <= 15000);
  assert.ok(Number.isInteger(cleanupBudgetMs) && cleanupBudgetMs > 0 && cleanupBudgetMs <= 2000);
  const deadline = performance.now() + operationBudgetMs;
  const result = { diagnosticOnly: true, status: 'error', phase: 'connect', raw: {}, groups: [] };
  async function step(phase, fn) {
    result.phase = phase;
    const remaining = deadline - performance.now();
    if (remaining <= 0) { const e = new Error('Deadline exceeded: ' + phase); e.name = 'TimeoutError'; throw e; }
    return bounded(fn, remaining, phase);
  }
  try {
    await step('connect', () => admin.connect());
    // Seed connect alone does not populate BrokerPool. Reuse the allowed read
    // to initialize topic metadata; this snapshot is NOT the later lag endpoint.
    const bootstrap = await step('bootstrapTopicOffsets', () => admin.fetchTopicOffsets(TOPIC));
    result.raw.bootstrap = bootstrap;
    partitions(bootstrap);
    for (const p of bootstrap) { numeric(p.offset); numeric(p.high); numeric(p.low); assert.equal(p.offset, p.high); assert.ok(Number(p.low) <= Number(p.high)); }
    const description = await step('describeGroups', () => admin.describeGroups([...GROUPS]));
    result.raw.describeGroups = description;
    assert.ok(Array.isArray(description.groups));
    assert.deepEqual(description.groups.map(g => g.groupId).sort(), [...GROUPS].sort());
    for (const groupId of GROUPS) {
      const g = description.groups.find(g => g.groupId === groupId);
      assert.ok(['Unknown', 'PreparingRebalance', 'CompletingRebalance', 'Stable', 'Dead', 'Empty'].includes(g.state) && g.state !== 'Unknown');
      assert.ok(g.errorCode === undefined || g.errorCode === 0);
      assert.equal(typeof g.protocol, 'string'); assert.equal(typeof g.protocolType, 'string'); assert.ok(Array.isArray(g.members));
      const members = g.members.map(m => {
        assert.equal(typeof m.memberId, 'string'); assert.ok(m.memberId.length && Buffer.isBuffer(m.memberAssignment));
        const decoded = decode(m.memberAssignment);
        assert.ok(decoded && decoded.version === 0 && decoded.assignment && typeof decoded.assignment === 'object' && !Array.isArray(decoded.assignment));
        for (const [topic, ps] of Object.entries(decoded.assignment)) {
          assert.equal(topic, TOPIC); assert.ok(Array.isArray(ps) && ps.every(p => p === 0 || p === 1)); assert.equal(new Set(ps).size, ps.length);
        }
        return { memberId: m.memberId, assignment: decoded.assignment };
      });
      assert.equal(new Set(members.map(m => m.memberId)).size, members.length);
      result.groups.push({ groupId, state: g.state, members });
    }
    for (const g of result.groups) {
      const raw = await step('fetchOffsets:' + g.groupId, () => admin.fetchOffsets({ groupId: g.groupId, topics: [TOPIC], resolveOffsets: false }));
      (result.raw.offsets ||= {})[g.groupId] = raw;
      assert.ok(Array.isArray(raw) && raw.length === 1 && raw[0].topic === TOPIC);
      g.offsets = partitions(raw[0].partitions).map(p => ({ partition: p.partition, current: numeric(p.offset) }));
    }
    const end = await step('fetchTopicOffsets', () => admin.fetchTopicOffsets(TOPIC)); result.raw.end = end;
    partitions(end);
    for (const p of end) { numeric(p.offset); numeric(p.high); numeric(p.low); assert.equal(p.offset, p.high); assert.ok(Number(p.low) <= Number(p.high)); }
    for (const g of result.groups) for (const p of g.offsets) {
      const e = end.find(e => e.partition === p.partition); p.end = numeric(e.high); p.low = numeric(e.low); assert.ok(p.current <= p.end); p.lag = p.end - p.current;
    }
    result.status = 'ok';
  } catch (e) { result.error = errorInfo(e); }
  finally {
    try { await bounded(() => admin.disconnect(), cleanupBudgetMs, 'disconnect'); }
    catch (e) { result.cleanupError = errorInfo(e); if (!result.error) { result.phase = 'disconnect'; result.error = result.cleanupError; } result.status = 'error'; }
  }
  return result;
}
const run = probe;
module.exports = { probe, run, config, GROUPS };
if (require.main === module || (module.id === '[stdin]' && process.argv[1] === '-')) {
  // stdin execution: node - BROKER:PORT; require resolves from API working directory.
  const watchdog = setTimeout(() => {
    process.stdout.write(JSON.stringify({ diagnosticOnly: true, status: 'error', phase: 'process-watchdog', error: { name: 'TimeoutError', message: 'Process deadline; cleanup not confirmed' } }) + '\n');
    process.exit(1);
  }, 19000);
  (async () => {
    const { Kafka, AssignerProtocol } = require('kafkajs');
    const kafka = new Kafka(config(process.argv.slice(2)));
    const admin = kafka.admin({ retry: { retries: 0 } });
    return probe({ admin, decode: b => AssignerProtocol.MemberAssignment.decode(b) });
  })().catch(e => ({ diagnosticOnly: true, status: 'error', phase: 'setup', error: errorInfo(e) })).then(result => {
    clearTimeout(watchdog);
    process.stdout.write(JSON.stringify(result) + '\n', () => process.exit(result.status === 'ok' ? 0 : 1));
  });
}
