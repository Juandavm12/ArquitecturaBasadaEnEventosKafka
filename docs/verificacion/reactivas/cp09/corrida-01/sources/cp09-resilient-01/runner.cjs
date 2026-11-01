'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const { setTimeout: pause } = require('node:timers/promises');
const v = require('./verify.cjs');
const preflightAdapter = require('./preflight-adapter.cjs');
const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const stage = __dirname, base = path.resolve(stage, '../../..');
const repo = path.join(base, 'entregas/ArquitecturaBasadaEnEventosKafka');
const runtime = path.join(stage, 'runtime');
const project = 'qa-taller-integracion-kafka';
function parse(text) {
  try { return JSON.parse(text); }
  catch (cause) { throw new Error('Invalid JSON receipt', { cause }); }
}
function normalizeInspect(items, requireHealthy = true) {
  assert.ok(Array.isArray(items)); assert.equal(items.length, 6);
  assert.equal(new Set(items.map(i => i.Id)).size, 6);
  const result = {};
  for (const i of items) {
    assert.ok(typeof i.Id === 'string' && i.Id.length > 0);
    assert.equal(i.Config.Labels['com.docker.compose.project'], project);
    const service = i.Config.Labels['com.docker.compose.service'];
    assert.ok(v.services.includes(service) && !result[service]);
    assert.ok(!i.State.Dead && !i.State.Paused && !i.State.Restarting);
    result[service] = { id: i.Id, image: i.Image, env: i.Config.Env, mounts: v.mounts(i.Mounts),
      hostname: i.Config.Hostname, configSHA: sha(JSON.stringify(i.Config)), hostConfigSHA: sha(JSON.stringify(i.HostConfig)),
      restartCount: i.RestartCount, startedAt: i.State.StartedAt, finishedAt: i.State.FinishedAt,
      state: i.State.Status, health: i.State.Health?.Status || null };
  }
  if (requireHealthy) v.healthy(result); return result;
}
function loadReference(io = fs) {
  return preflightAdapter.loadReference(normalizeInspect, io);
}
function matchReference(historic, fresh, reference) {
  for (const s of v.services) for (const field of ['id', 'image', 'env', 'mounts'])
    v.same(field === 'mounts' ? v.mounts(historic[s][field]) : historic[s][field],
      field === 'mounts' ? v.mounts(reference.snapshot[s][field]) : reference.snapshot[s][field]);
  v.same(reference.snapshot, fresh);
}
function createAdapter({ directory = runtime, execute = spawnSync, sleep = pause } = {}) {
  let seq = 0;
  const phase = process.argv[2];
  const put = (name, data) => fs.writeFileSync(path.join(directory, name), data, { flag: 'wx' });
  const save = (name, data) => put(name + '.json', JSON.stringify(data, null, 2) + '\n');
  const load = name => parse(fs.readFileSync(path.join(directory, name + '.json'), 'utf8'));
  function command(argv, timeout = 25000, inputBuffer) {
    assert.ok(Number.isFinite(timeout) && timeout > 0 && timeout <= 30000);
    assert.ok(inputBuffer === undefined || Buffer.isBuffer(inputBuffer));
    const name = `${phase}-${String(++seq).padStart(4, '0')}`;
    const startUTC = new Date().toISOString(), start = process.hrtime.bigint();
    let result;
    try { result = execute(argv[0], argv.slice(1), { cwd: path.join(repo, 'kafka'), encoding: 'utf8', input: inputBuffer, timeout, maxBuffer: 32 * 1024 * 1024 }); }
    catch (error) { result = { error, status: null }; }
    const stdout = result.stdout || '', stderr = result.stderr || '';
    put(name + '-stdout.txt', stdout); put(name + '-stderr.txt', stderr);
    save(name + '-command', { argv, cwd: path.join(repo, 'kafka'), startUTC, endUTC: new Date().toISOString(), durationMs: Number(process.hrtime.bigint() - start) / 1e6, exitCode: result.status, signal: result.signal || null, error: result.error?.message || null, stdinSHA256: inputBuffer === undefined ? null : sha(inputBuffer) });
    assert.ok(!result.error && !result.signal && result.status === 0, `${name}: exit=${result.status} ${result.error?.message || ''} ${stderr}`);
    return { stdout, stderr };
  }
  const docker = args => command(['docker', ...args]);
  function snapshot() {
    const ids = docker(['ps', '-aq', '--filter', 'label=com.docker.compose.project=' + project]).stdout.trim().split(/\s+/);
    assert.equal(ids.length, 6); assert.equal(new Set(ids).size, 6);
    return normalizeInspect(parse(docker(['inspect', ...ids]).stdout), false);
  }
  function sources() {
    const result = {};
    function walk(dir) {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        if (['.git', 'node_modules'].includes(entry.name)) continue;
        const file = path.join(dir, entry.name);
        assert.ok(!/^\.env(?:\.|$)|\.(?:pem|key|p12|pfx)$|credentials|secrets/i.test(entry.name), 'Sensitive source location: refusing to read ' + file);
        assert.ok(!entry.isSymbolicLink(), 'Source symlink forbidden: ' + file);
        if (entry.isDirectory()) walk(file);
        else if (entry.isFile()) result[file] = sha(fs.readFileSync(file));
      }
    }
    walk(repo);
    const plan = path.join(base, 'PLAN_DE_TRABAJO.md');
    const text = fs.readFileSync(plan, 'utf8'); result[plan] = sha(text);
    const plans = { [plan]: text };
    for (const relative of ['README.md', 'PLAN_KAFKA.md', 'docs/verificacion/consumidores/RESUMEN.md']) {
      const file = path.join(repo, relative); plans[file] = fs.readFileSync(file, 'utf8');
    }
    return { hashes: result, plans };
  }
  function unchanged() { v.same(load('sources-before'), sources()); }
  function groups(snap, active = true) {
    return preflightAdapter.queryGroups(snap, active, command,
      source => put(`groups-source-${String(seq + 1).padStart(4, '0')}.cjs`, source), normalizeInspect);
  }
  function logs(snap) {
    return Object.fromEntries(['orders-api', 'inventory-consumer', 'billing-consumer', 'notification-consumer'].map(s => {
      const streams = docker(['logs', '--timestamps', snap[s].id]);
      // Raw streams are immutable command receipts; concatenation is parsing input only.
      return [s, streams.stdout + '\n' + streams.stderr];
    }));
  }
  async function waitHealthy(original, afterStart) {
    const deadline = Date.now() + 90000;
    for (;;) {
      const current = snapshot();
      v.restorationState(original, afterStart, current);
      if (current['inventory-consumer'].state === 'running' && current['inventory-consumer'].health === 'healthy') return current;
      assert.ok(Date.now() < deadline, 'Inventory not healthy within 90s'); await sleep(1000);
    }
  }
  async function restore(pre) {
    // Capture is best effort; it must NEVER prevent the single start attempt.
    let captureError;
    try { save('before-restore', { snapshot: snapshot(), logs: logs(pre.snapshot) }); }
    catch (error) { captureError = error.message; }
    finally {
      save('restore-attempt-once', { id: pre.snapshot['inventory-consumer'].id, atUTC: new Date().toISOString(), captureError: captureError || null });
      docker(['start', pre.snapshot['inventory-consumer'].id]);
    }
    const afterStart = snapshot();
    save('after-explicit-start', afterStart);
    v.restorationState(pre.snapshot, afterStart, afterStart);
    const snap = await waitHealthy(pre.snapshot, afterStart); v.healthy(snap);
    let restoredGroups, restoredLogs;
    const deadline = Date.now() + 30000;
    for (;;) {
      restoredGroups = groups(snap); restoredLogs = logs(snap);
      if (Object.values(restoredGroups).every(partitions => partitions.every(p => p.lag === 0))) break;
      assert.ok(Date.now() < deadline, 'Restored consumers have not caught up'); await sleep(500);
    }
    unchanged();
    save('restored-complete', { snapshot: snap, afterExplicitStart: afterStart, groups: restoredGroups, logs: restoredLogs, sourcesUnchanged: true });
  }
  async function preflight() {
    save('preflight.lock', { atUTC: new Date().toISOString() });
    const reference = loadReference();
    const cp07 = parse(fs.readFileSync(path.join(stage, '../cp07-responsive-01/runtime/restored.json'), 'utf8'));
    matchReference(cp07, reference.snapshot, reference);
    save('sources-before', sources());
    command(['postman', '--version']); command(['postman', 'collection', 'run', '--help']);
    const snap = snapshot(); v.healthy(snap);
    matchReference(cp07, snap, reference);
    const beforeGroups = groups(snap);
    for (const partitions of Object.values(beforeGroups)) for (const p of partitions) assert.equal(p.lag, 0);
    const collectionSHA256 = sha(fs.readFileSync(path.join(stage, 'CP09.postman_collection.json')));
    unchanged(); save('preflight-complete', { snapshot: snap, groups: beforeGroups, collectionSHA256, provenance: reference.provenance, atUTC: new Date().toISOString() });
  }
  async function run() {
    // Lazy import: importing this adapter never invokes the parallel flow or an app.
    const { once, exercise } = require('./flow.cjs');
    let pre, before, empty, postman;
    await exercise({
      acquire: async () => {
        once(path.join(directory, 'run.lock'), { atUTC: new Date().toISOString() });
        assert.ok(!fs.existsSync(path.join(directory, 'post-attempt-once.json')), 'POST guard already exists');
        assert.ok(!fs.existsSync(path.join(directory, 'restore-attempt-once.json')), 'Restoration already attempted');
        pre = load('preflight-complete'); unchanged();
        assert.equal(sha(fs.readFileSync(path.join(stage, 'CP09.postman_collection.json'))), pre.collectionSHA256);
        const fresh = snapshot(); v.same(pre.snapshot, fresh);
      },
      stop: async () => {
        save('stop-attempt-once', { id: pre.snapshot['inventory-consumer'].id, atUTC: new Date().toISOString() });
        docker(['stop', '--time', '15', pre.snapshot['inventory-consumer'].id]);
        before = snapshot(); v.windowState(pre.snapshot, before);
        empty = groups(before, false).inventory;
        for (let i = 0; i < 2; i++) { assert.equal(empty[i].lag, 0); assert.equal(empty[i].current, pre.groups.inventory[i].current); assert.equal(empty[i].end, pre.groups.inventory[i].end); }
        save('window-before-http', { snapshot: before, inventory: empty });
      },
      publish: async () => {
        const collection = fs.readFileSync(path.join(stage, 'CP09.postman_collection.json'));
        assert.equal(sha(collection), pre.collectionSHA256);
        put('collection-executed.json', collection);
        once(path.join(directory, 'post-attempt-once.json'), { authorizedRequests: 1, collectionSHA256: sha(collection), pinnedID: pre.snapshot['inventory-consumer'].id, atUTC: new Date().toISOString() });
        postman = command(['postman', 'collection', 'run', path.join(directory, 'collection-executed.json'), '--iteration-count', '1', '--ignore-redirects', '--no-report-events', '--timeout-request', '10000', '--timeout-script', '5000', '--timeout', '20000'], 30000).stdout;
        v.http(postman);
      },
      collect: async () => {
        const response = v.http(postman), deadline = Date.now() + 30000;
        let observed;
        for (;;) {
          observed = logs(before);
          const ready = ['billing', 'notification'].every(role => v.rows(observed[role + '-consumer']).some(r => r.phase === 'offset-committed' && r.trace?.orderId === response.orderId));
          if (ready) break;
          assert.ok(Date.now() < deadline, 'Missing consumer commits'); await sleep(500);
        }
        const g = groups(before, false), after = snapshot();
        v.windowState(pre.snapshot, after); v.same(before['inventory-consumer'], after['inventory-consumer']);
        save('collected', { postman, before, after, empty, groups: g, logs: observed });
        unchanged(); return { collected: true };
      },
      restore: async () => restore(pre)
    });
    const result = v.verify(directory); save('result', result); return result;
  }
  async function rescue() {
    save('rescue.lock', { atUTC: new Date().toISOString() });
    assert.ok(fs.existsSync(path.join(directory, 'stop-attempt-once.json')), 'No authorized stop receipt');
    assert.ok(!fs.existsSync(path.join(directory, 'restore-attempt-once.json')), 'Never repeat a start attempt');
    const pre = load('preflight-complete'), current = snapshot();
    v.windowState(pre.snapshot, current); await restore(pre);
    return { status: 'RESTORED_ONLY', publication: 'not attempted by rescue' };
  }
  return { command, snapshot, preflight, run, rescue };
}
async function main() {
  const phase = process.argv[2]; assert.ok(['preflight', 'run', 'restore'].includes(phase), 'Use preflight | run | restore');
  fs.mkdirSync(runtime, { recursive: true });
  const adapter = createAdapter();
  try {
    const result = await adapter[phase === 'restore' ? 'rescue' : phase]();
    console.log(JSON.stringify(result || { status: 'PREFLIGHT_ONLY' }));
  } catch (error) {
    const failure = { status: 'FAILED', atUTC: new Date().toISOString(), error: error.stack, causes: error.errors?.map(e => e.stack), restorationStatus: fs.existsSync(path.join(runtime, 'restored-complete.json')) ? 'RESTORED' : 'NOT_CONFIRMED' };
    fs.writeFileSync(path.join(runtime, `${phase}-failure-${Date.now()}-${crypto.randomUUID()}.json`), JSON.stringify(failure, null, 2) + '\n', { flag: 'wx' });
    console.error(JSON.stringify(failure)); process.exitCode = 1;
  }
}
module.exports = { createAdapter, loadReference, normalizeInspect, matchReference };
if (require.main === module) main().catch(error => { console.error(error.stack); process.exitCode = 1; });
