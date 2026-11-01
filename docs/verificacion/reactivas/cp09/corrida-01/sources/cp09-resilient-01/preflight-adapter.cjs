'use strict';
// Import-inert. Administrative observations are sequential reads, NOT an atomic
// Kafka snapshot, a resumption PASS, or evidence that CP09 has been executed.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const v = require('./verify.cjs');
const CONTROL = path.resolve(__dirname, '../cp09-control-padre-01');
const OBS = 'grupos-kafkajs-01/observacion-04/';
const LEGACY = 'reanudacion-03/runtime/';
const PROBE = 'grupos-kafkajs-01/probe03.cjs';
const PROBE_SHA = '779ece5f726f4492b53f1f537be7da0c96fea0be37fba9d27fafece28700896c';
const HASHES = Object.freeze({
  'grupos-kafkajs-01/observacion-04/summary.json': 'b0b323dbb2cc5a119d601b2c094e245d39217592c139b87550c430501f2a44b7',
  'grupos-kafkajs-01/observacion-04/admission.json': 'f834cc5266c700d05a836f3e5c7e022f6510693dd9703aed21e6cf5967981503',
  'grupos-kafkajs-01/observacion-04/validation-parent.json': '2c85bbaea9f033d567b67a42a731df1b8b87fd275c6a2cd1bfe59d325dc6cc0a',
  'grupos-kafkajs-01/observacion-04/response.json': 'a7842b10c02612010765e1f2a98792a09df758f20013ce16f561cc9ef92dd09e',
  'grupos-kafkajs-01/observacion-04/probe-executed.cjs': PROBE_SHA,
  'grupos-kafkajs-01/observacion-04/01-command.json': 'dae3f8e745c4294bb49e8b9d32d231189e23189291462be3453ef88e2742429b',
  'grupos-kafkajs-01/observacion-04/01-stdout.txt': '6c0a56deb12641282dccb7e0ae03d861d84573e0217a0bfef52ad279e838dd1a',
  'grupos-kafkajs-01/observacion-04/01-stderr.txt': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
  'grupos-kafkajs-01/observacion-04/02-command.json': '265e1c838b0d855fd39257923fce1caa558d852f25122498af2a0b8dd8973e1a',
  'grupos-kafkajs-01/observacion-04/02-stdout.txt': 'd77170201ad00d51ccb1d6d04cb72dec5f3d695356ec9e45a7d07ac5d204a057',
  'grupos-kafkajs-01/observacion-04/02-stderr.txt': '3e94330c70f8646d3a18b9240e617919e0b28198212739a3217a29cc482196aa',
  'grupos-kafkajs-01/observacion-04/03-command.json': '5747fbd70fe5e84b9fb07a668fedf0d19e8701eedf3a892fe81fa39ddd7f93c8',
  'grupos-kafkajs-01/observacion-04/03-stdout.txt': '42c34ee49c983a661edf4708b8efb28f7341e986807229348b6b709d7ca22aef',
  'grupos-kafkajs-01/observacion-04/03-stderr.txt': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
  'grupos-kafkajs-01/observacion-01/01-stdout.txt': '82f5ce9bc2620a9dd48feba429bea2479ccf70f33d064e43d862796743cf35d5',
  'grupos-kafkajs-01/probe03.cjs': PROBE_SHA,
  'reanudacion-03/runtime/result.json': '856fc5b8349c7bc42d0bf34b53a1e1eb2af31db17bae0b4cbd396f845848687f',
  'reanudacion-03/runtime/after-inspect.json': '0aba55eddb72044e8f185e6880f2fa5d3b546515ee357a9567f99a21bdd56259',
  'reanudacion-03/runtime/healthy-baseline.json': '4be6ea77abca72212e71996a28d4669af3dff36a7428cf3af11814be6b7212c6'
});
const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const parse = bytes => JSON.parse(bytes.toString());
const canonical = value => JSON.stringify(value, (_, x) => x && typeof x === 'object' && !Array.isArray(x)
  ? Object.fromEntries(Object.keys(x).sort().map(k => [k,x[k]])) : x);
function readPinned(name, io) {
  assert.ok(Object.hasOwn(HASHES,name));
  const file = path.join(CONTROL,name);
  let current = path.parse(file).root;
  const parts = file.slice(current.length).split(path.sep);
  for (const [index,part] of parts.entries()) {
    current = path.join(current,part);
    const stat = io.lstatSync(current);
    assert.ok(!stat.isSymbolicLink(),'Reference symlink forbidden: ' + current);
    assert.ok(index === parts.length-1 ? stat.isFile() : stat.isDirectory());
  }
  const bytes = io.readFileSync(file);
  assert.equal(sha(bytes),HASHES[name],'Pinned SHA mismatch: ' + name);
  return bytes;
}
function loadReference(normalizeInspect, io = fs) {
  const bytes = Object.fromEntries(Object.keys(HASHES).map(n => [n,readPinned(n,io)]));
  const json = n => parse(bytes[n]);
  const summary = json(OBS+'summary.json'), admission = json(OBS+'admission.json');
  const validation = json(OBS+'validation-parent.json');
  assert.equal(summary.diagnosticOnly,true); assert.equal(summary.status,'diagnostic-ok');
  assert.equal(summary.cp09Executed,false); assert.equal(summary.adminQueriesAttempted,1);
  assert.equal(summary.primaryError,null); assert.equal(summary.afterSnapshotError,null);
  for (const item of [summary,validation]) for (const action of ['post','start','stop']) assert.equal(item[action],0);
  for (const counts of [summary.originalsBefore,summary.originalsAfter,validation.protected])
    assert.deepEqual(counts,{originals:566,plans:4});
  assert.equal(validation.status,'PASS');
  assert.equal(validation.scope,'administrative diagnostic only; CP09 not executed');
  assert.equal(validation.sourceSHA256,PROBE_SHA); assert.equal(admission.executedProbeSHA256,PROBE_SHA);
  assert.equal(admission.version,'observation04');
  assert.equal(admission.referenceSHA256,HASHES['grupos-kafkajs-01/observacion-01/01-stdout.txt']);
  assert.ok(bytes[PROBE].equals(bytes[OBS+'probe-executed.cjs']));
  const before = json(OBS+'01-stdout.txt'), after = json(OBS+'03-stdout.txt');
  const snapshot = normalizeInspect(before);
  v.same(snapshot,normalizeInspect(after));
  v.same(snapshot,normalizeInspect(json('grupos-kafkajs-01/observacion-01/01-stdout.txt')));
  for (const [service,[count,startedAt]] of Object.entries(admission.counts)) {
    assert.equal(snapshot[service].restartCount,count); assert.equal(snapshot[service].startedAt,startedAt);
  }
  const ids = before.map(i=>i.Id);
  for (const n of ['01','02','03']) {
    const receipt = json(OBS+n+'-command.json');
    assert.equal(receipt.status,0); assert.equal(receipt.error,null); assert.equal(receipt.signal,null);
    assert.ok(Number.isFinite(receipt.durationMs) && receipt.durationMs>=0 && receipt.durationMs<=25000);
    assert.ok(Number.isFinite(Date.parse(receipt.startUTC)) && Date.parse(receipt.endUTC)>=Date.parse(receipt.startUTC));
    assert.ok(Buffer.from(receipt.stdout).equals(bytes[OBS+n+'-stdout.txt']));
    assert.ok(Buffer.from(receipt.stderr).equals(bytes[OBS+n+'-stderr.txt']));
    assert.deepEqual(receipt.argv,n==='02' ? queryArgv(snapshot) : ['docker','inspect',...ids]);
    assert.equal(receipt.stdinSHA256,n==='02' ? PROBE_SHA : null);
  }
  const response = json(OBS+'response.json');
  assert.deepEqual(response,json(OBS+'02-stdout.txt')); assert.deepEqual(response,summary.response);
  const legacy = json(LEGACY+'result.json'); assert.equal(legacy.status,'PASS');
  assert.equal(legacy.healthyBaselineSHA256,HASHES[LEGACY+'healthy-baseline.json']);
  assert.equal(legacy.rawFinalSHA256,HASHES[LEGACY+'after-inspect.json']);
  const oldRaw = json(LEGACY+'after-inspect.json');
  v.same(normalizeInspect(oldRaw),json(LEGACY+'healthy-baseline.json'));
  // Observed restarts may change State/count, never recreate or configuration.
  for (const item of before) {
    const old = oldRaw.find(i=>i.Id===item.Id); assert.ok(old,'Legacy container ID missing');
    for (const field of ['Id','Image','Config','HostConfig']) assert.equal(canonical(item[field]),canonical(old[field]),field);
    assert.equal(canonical(v.mounts(item.Mounts)),canonical(v.mounts(old.Mounts)));
  }
  const network = {};
  for (const role of ['inventory','billing','notification']) {
    const service=role+'-consumer', item=before.find(i=>i.Id===snapshot[service].id);
    const ips=Object.values(item.NetworkSettings.Networks).map(n=>n.IPAddress).filter(Boolean);
    assert.equal(ips.length,1,'Ambiguous archived network');
    network[role]={id:item.Id,hostname:item.Config.Hostname,ip:ips[0]};
  }
  normalizeGroups(response,snapshot,true,network);
  return {snapshot,network,provenance:{kind:'administrative-observation',path:path.join(CONTROL,OBS),
    diagnosticOnly:true,cp09Executed:false,sourceSHA256:PROBE_SHA,receiptSHA256:{...HASHES},
    scope:'sequential administrative reads; not atomic; not CP09 PASS'}};
}
function queryArgv(snapshot) {
  return ['docker','exec','-i','--workdir','/app',snapshot['orders-api'].id,'node','-','kafka:9092'];
}
function numeric(value) {
  assert.equal(typeof value,'string'); assert.match(value,/^(0|[1-9]\d*)$/);
  const n=Number(value); assert.ok(Number.isSafeInteger(n)); return n;
}
function partitions(rows) {
  assert.ok(Array.isArray(rows)); assert.deepEqual(rows.map(p=>p.partition).sort(),[0,1]); return rows;
}
function keys(object, expected) { assert.deepEqual(Object.keys(object).sort(),[...expected].sort()); }
function normalizeGroups(value,snapshot,active,network) {
  assert.equal(typeof active,'boolean'); assert.equal(value.diagnosticOnly,true); assert.equal(value.status,'ok');
  keys(value,['diagnosticOnly','status','phase','raw','groups']);
  for (const key of ['error','cleanupError','signal']) assert.ok(!Object.hasOwn(value,key),'Unexpected ' + key);
  assert.equal(value.phase,'fetchTopicOffsets');
  const roles=['inventory','billing','notification'], ids=roles.map(r=>r+'-service-group');
  assert.ok(Array.isArray(value.groups)); assert.deepEqual(value.groups.map(g=>g.groupId).sort(),[...ids].sort());
  const raw=value.raw; assert.ok(raw && raw.describeGroups);
  keys(raw,['bootstrap','describeGroups','offsets','end']);
  const memberIds=value.groups.flatMap(g=>g.members.map(m=>m.memberId));
  assert.equal(new Set(memberIds).size,memberIds.length,'Duplicate membership');
  assert.deepEqual(raw.describeGroups.groups.map(g=>g.groupId).sort(),[...ids].sort());
  keys(raw.offsets,ids);
  for (const rows of [raw.bootstrap,raw.end]) for (const p of partitions(rows)) {
    assert.equal(numeric(p.offset),numeric(p.high)); assert.ok(numeric(p.low)<=numeric(p.high));
  }
  const result={};
  for (const role of roles) {
    const id=role+'-service-group', g=value.groups.find(g=>g.groupId===id), r=raw.describeGroups.groups.find(g=>g.groupId===id);
    keys(g,['groupId','state','members','offsets']);
    const present=role==='inventory' ? active : true, count=present ? 1 : 0;
    assert.equal(g.state,present?'Stable':'Empty'); assert.equal(r.state,g.state);
    assert.ok(r.errorCode===undefined || r.errorCode===0);
    assert.ok(present ? r.protocolType==='consumer' : ['', 'consumer'].includes(r.protocolType));
    assert.ok(present ? r.protocol==='RoundRobinAssigner' : ['', 'RoundRobinAssigner'].includes(r.protocol));
    assert.ok(Array.isArray(g.members) && Array.isArray(r.members)); assert.equal(g.members.length,count); assert.equal(r.members.length,count);
    const pin=network[role], service=snapshot[role+'-consumer'];
    assert.equal(service.id,pin.id); assert.equal(service.hostname,pin.hostname);
    let member='-';
    if(present) {
      const m=g.members[0], rm=r.members[0];
      keys(m,['memberId','assignment']);
      assert.equal(typeof m.memberId,'string'); assert.ok(m.memberId.length && m.memberId !== '-' && !/\s/.test(m.memberId)); assert.equal(m.memberId,rm.memberId);
      assert.equal(rm.clientId,role+'-consumer-'+service.hostname); assert.equal(rm.clientHost,'/'+pin.ip);
      assert.deepEqual(m.assignment,{'orders.events':[0,1]});
      // Frozen probe uses KafkaJS' decoder. Compare its decoded assignment to the
      // exact version-0 encoding for this sole authorized [0,1] assignment.
      assert.deepEqual(rm.memberAssignment,{type:'Buffer',data:[0,0,0,0,0,1,0,13,111,114,100,101,114,115,46,101,118,101,110,116,115,0,0,0,2,0,0,0,0,0,0,0,1,0,0,0,0]});
      member=m.memberId;
    }
    const offsets=raw.offsets[id]; assert.ok(Array.isArray(offsets)); assert.equal(offsets.length,1); assert.equal(offsets[0].topic,'orders.events');
    partitions(offsets[0].partitions);
    result[role]=partitions(g.offsets).map(p=>{
      keys(p,['partition','current','end','low','lag']);
      for(const k of ['partition','current','end','low','lag']) assert.ok(Number.isSafeInteger(p[k]) && p[k]>=0);
      const committed=offsets[0].partitions.find(x=>x.partition===p.partition), end=raw.end.find(x=>x.partition===p.partition);
      assert.equal(p.current,numeric(committed.offset)); assert.equal(p.end,numeric(end.high)); assert.equal(p.low,numeric(end.low));
      assert.ok(p.current<=p.end); assert.equal(p.lag,p.end-p.current);
      return {partition:p.partition,current:p.current,end:p.end,lag:p.lag,member};
    }).sort((a,b)=>a.partition-b.partition);
  }
  return result;
}
function queryGroups(snapshot,active,command,archive,normalizeInspect,io=fs) {
  const reference=loadReference(normalizeInspect,io);
  assert.equal(snapshot['orders-api'].id,reference.snapshot['orders-api'].id);
  const source=readPinned(PROBE,io);
  archive(source); // Must be immutable WX and precede the single query attempt.
  const streams=command(queryArgv(snapshot),25000,source);
  return normalizeGroups(parse(streams.stdout),snapshot,active,reference.network);
}
module.exports={CONTROL,HASHES,PROBE_SHA,loadReference,normalizeGroups,queryGroups};
