'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const services = ['kafka', 'kafka-ui', 'orders-api', 'inventory-consumer', 'billing-consumer', 'notification-consumer'];
function parse(text) {
  try { return JSON.parse(text); }
  catch (cause) { throw new Error('Malformed JSON evidence', { cause }); }
}
function marker(stdout, key) {
  const text = stdout.replace(/\r/g, '').replace(/\n\s*[|│] /g, '');
  const matches = [...text.matchAll(new RegExp("(?:^|[\\s'\"’])" + key + ' ', 'g'))];
  assert.equal(matches.length, 1, 'Missing/duplicate ' + key);
  const tail = text.slice(matches[0].index + matches[0][0].length);
  const end = tail.search(/['”’](?=\s*(?:$|['"]?[A-Z0-9_]+ ))|\n/);
  return (end < 0 ? tail : tail.slice(0, end)).trim().replace(/['”’]$/, '');
}
function rows(text) {
  return text.split('\n').flatMap(line => {
    const match = line.match(/^\S+ (\{.*\})\s*$/);
    return match ? [parse(match[1])] : [];
  });
}
function group(text, role, active) {
  const found = text.split('\n').map(line => line.trim().split(/\s+/))
    .filter(parts => parts[0] === role + '-service-group' && parts[1] === 'orders.events');
  assert.equal(found.length, 2, role + ': expected exactly two partitions');
  const result = found.map(p => {
    assert.ok(p.length >= 9);
    for (const value of p.slice(2, 6)) assert.match(value, /^\d+$/, 'Unknown Kafka numeric value');
    const [partition, current, end, lag] = p.slice(2, 6).map(Number);
    assert.ok([partition, current, end, lag].every(Number.isSafeInteger));
    assert.equal(end - current, lag);
    assert.equal(p[6] !== '-', active, 'Unexpected group membership');
    return { partition, current, end, lag, member: p[6] };
  }).sort((a, b) => a.partition - b.partition);
  assert.deepEqual(result.map(p => p.partition), [0, 1]); return result;
}
function same(before, after) { assert.deepEqual(after, before, 'Pinned container changed'); }
const canonical = value => JSON.stringify(value, function (_, v) {
  return v && typeof v === 'object' && !Array.isArray(v)
    ? Object.fromEntries(Object.keys(v).sort().map(k => [k, v[k]])) : v;
});
const mounts = value => [...value].sort((a, b) => canonical(a) < canonical(b) ? -1 : canonical(a) > canonical(b) ? 1 : 0);
function identity(item) {
  return Object.fromEntries(Object.entries(item).filter(([key]) => !['state', 'health', 'startedAt', 'finishedAt', 'restartCount'].includes(key))
    .map(([key, value]) => [key, key === 'mounts' ? mounts(value) : value]));
}
function healthy(snapshot) {
  assert.deepEqual(Object.keys(snapshot).sort(), [...services].sort());
  for (const service of services) {
    assert.equal(snapshot[service].state, 'running', service);
    if (service !== 'kafka-ui') assert.equal(snapshot[service].health, 'healthy', service);
  }
  assert.ok(snapshot['inventory-consumer'].env.includes('PROCESSING_DELAY_MS=0'));
}
function windowState(before, after) {
  for (const s of services.filter(s => s !== 'inventory-consumer')) same(before[s], after[s]);
  const inventory = after['inventory-consumer'];
  same(identity(before['inventory-consumer']), identity(inventory));
  assert.equal(inventory.state, 'exited');
  assert.equal(inventory.startedAt, before['inventory-consumer'].startedAt);
  assert.equal(inventory.restartCount, before['inventory-consumer'].restartCount);
}
function restorationState(before, afterStart, final) {
  for (const snapshot of [before, afterStart, final])
    assert.deepEqual(Object.keys(snapshot).sort(), [...services].sort());
  for (const s of services) {
    if (s !== 'inventory-consumer') { same(before[s], afterStart[s]); same(before[s], final[s]); continue; }
    const reference = afterStart[s];
    same(identity(before[s]), identity(reference)); same(identity(reference), identity(final[s]));
    assert.equal(reference.state, 'running'); assert.equal(final[s].state, 'running');
    assert.ok(Number.isSafeInteger(reference.restartCount) && reference.restartCount >= 0);
    assert.ok(Number.isFinite(Date.parse(reference.startedAt)) && Date.parse(reference.startedAt) > Date.parse(before[s].startedAt), 'New start timestamp required');
    assert.equal(final[s].restartCount, reference.restartCount);
    assert.equal(final[s].startedAt, reference.startedAt);
  }
}
function http(stdout) {
  assert.equal(marker(stdout, 'REQUEST_METHOD'), 'POST');
  assert.equal(marker(stdout, 'REQUEST_URL'), 'http://localhost:3000/orders');
  assert.equal(parse(marker(stdout, 'REQUEST_BODY')).customerName, 'Pedro CP09');
  assert.equal(marker(stdout, 'RESPONSE_CODE'), '202');
  const response = parse(marker(stdout, 'RESPONSE_BODY'));
  assert.equal(response.status, 'accepted');
  assert.equal(response.message, 'Pedido recibido y evento OrderCreated publicado en Kafka');
  assert.match(response.orderId, /^ORD-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
  assert.equal(marker(stdout, 'ORDER_ID'), response.orderId);
  const started = marker(stdout, 'CP09_REQUEST_STARTED_UTC');
  const observed = marker(stdout, 'CP09_RESPONSE_OBSERVED_UTC');
  for (const utc of [started, observed]) assert.match(utc, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/, 'Invalid UTC marker');
  const start = Date.parse(started);
  const end = Date.parse(observed);
  assert.ok(Number.isFinite(start) && Number.isFinite(end) && start <= end);
  const ms = Number(marker(stdout, 'RESPONSE_TIME_MS')); assert.ok(Number.isFinite(ms) && ms >= 0);
  return response;
}
function validateWindow({ logs, snapshot, publication }) {
  for (const key of ['orderId', 'eventId', 'correlationId', 'topic', 'partition', 'baseOffset']) assert.notEqual(publication[key], undefined);
  assert.equal(publication.topic, 'orders.events');
  assert.ok([0, 1].includes(publication.partition));
  assert.match(String(publication.baseOffset), /^\d+$/);
  assert.equal(rows(logs['inventory-consumer']).filter(r => r.trace?.orderId === publication.orderId).length, 0, 'Inventory traces while stopped');
  for (const role of ['billing', 'notification']) {
    const found = rows(logs[role + '-consumer']).filter(r => r.trace?.orderId === publication.orderId);
    assert.deepEqual(found.map(r => r.phase), ['received', 'processed', 'offset-committed']);
    for (const row of found) {
      for (const key of ['orderId', 'eventId', 'correlationId', 'topic', 'partition']) assert.equal(row.trace[key], publication[key], key);
      assert.equal(String(row.trace.offset), String(publication.baseOffset));
      assert.equal(row.trace.groupId, role + '-service-group');
      assert.equal(row.instanceId, snapshot[role + '-consumer'].hostname);
      assert.equal(row.trace.instanceId, row.instanceId);
    }
  }
  return { traceCount: 6 };
}
function offsets(before, empty, after, publication) {
  for (let i = 0; i < 2; i++) {
    assert.equal(before[i].lag, 0); assert.equal(empty[i].lag, 0);
    assert.equal(empty[i].current, before[i].current); assert.equal(empty[i].end, before[i].end);
    assert.equal(after[i].current, before[i].current);
    const delta = i === publication.partition ? 1 : 0;
    assert.equal(after[i].end, before[i].end + delta); assert.equal(after[i].lag, delta);
    if (delta) assert.equal(String(publication.baseOffset), String(before[i].current));
  }
}
function verify(directory) {
  const load = name => parse(fs.readFileSync(path.join(directory, name + '.json'), 'utf8'));
  const pre = load('preflight-complete'), collected = load('collected'), restored = load('restored-complete');
  const afterStart = load('after-explicit-start');
  same(afterStart, restored.afterExplicitStart);
  restorationState(pre.snapshot, afterStart, restored.snapshot);
  const response = http(collected.postman);
  const api = rows(collected.logs['orders-api']).filter(r => r.orderId === response.orderId);
  assert.equal(api.length, 1); const publication = api[0];
  windowState(pre.snapshot, collected.before); windowState(pre.snapshot, collected.after);
  same(collected.before['inventory-consumer'], collected.after['inventory-consumer']);
  validateWindow({ logs: collected.logs, snapshot: pre.snapshot, publication });
  offsets(pre.groups.inventory, collected.empty, collected.groups.inventory, publication);
  for (const role of ['billing', 'notification']) for (let i = 0; i < 2; i++) {
    const p = collected.groups[role][i], original = pre.groups[role][i];
    assert.equal(p.lag, 0); assert.equal(p.current, original.current + (i === publication.partition ? 1 : 0));
    assert.equal(p.end, original.end + (i === publication.partition ? 1 : 0));
  }
  healthy(restored.snapshot);
  assert.equal(restored.sourcesUnchanged, true);
  const originals = load('sources-before');
  for (const [file, hash] of Object.entries(originals.hashes)) {
    assert.equal(crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex'), hash, 'Source changed: ' + file);
  }
  for (const role of ['inventory', 'billing', 'notification']) for (let i = 0; i < 2; i++) {
    const final = restored.groups[role][i], original = pre.groups[role][i];
    assert.equal(final.lag, 0);
    assert.equal(final.current, original.current + (i === publication.partition ? 1 : 0));
    assert.equal(final.end, final.current);
  }
  for (const role of ['billing', 'notification']) {
    const select = text => rows(text).filter(r => r.trace?.orderId === response.orderId);
    same(select(collected.logs[role + '-consumer']), select(restored.logs[role + '-consumer']));
  }
  const recovered = rows(restored.logs['inventory-consumer']).filter(r => r.trace?.orderId === response.orderId);
  assert.deepEqual(recovered.map(r => r.phase), ['received', 'processed', 'offset-committed']);
  for (const row of recovered) {
    for (const key of ['orderId', 'eventId', 'correlationId', 'topic', 'partition']) assert.equal(row.trace[key], publication[key]);
    assert.equal(String(row.trace.offset), String(publication.baseOffset));
    assert.equal(row.trace.groupId, 'inventory-service-group');
    assert.equal(row.instanceId, restored.snapshot['inventory-consumer'].hostname);
    assert.equal(row.trace.instanceId, row.instanceId);
  }
  return { status: 'PASS', orderId: response.orderId, preservedSourceFiles: Object.keys(originals.hashes).length, preservedPlans: Object.keys(originals.plans).length, basis: 'Controlled stop; host sequence, not clock ordering or SLA. CP11/12 not closed.' };
}
module.exports = { services, marker, rows, group, same, identity, healthy, windowState, http, validateWindow, offsets, verify, mounts, restorationState };
if (require.main === module) {
  try { console.log(JSON.stringify(verify(path.join(__dirname, 'runtime')))); }
  catch (error) { console.error('FAILED: ' + error.stack); process.exitCode = 1; }
}
