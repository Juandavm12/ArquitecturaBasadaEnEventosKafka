'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {spawnSync} = require('node:child_process');
const target = path.join(__dirname, 'probe03.cjs');
const source = path.resolve(__dirname, '../diagnostico-kafkajs-02/image-source/src/cluster/brokerPool.js');
function pool() {
 class Broker {
  constructor(options) { Object.assign(this, options); this.connected=false; this.versions={}; }
  async connect() { this.connected=true; }
  async disconnect() { this.connected=false; }
  isConnected() { return this.connected; }
  async metadata() { return {brokers:[{nodeId:1,host:'kafka',port:9092}],topicMetadata:[{topic:'orders.events'}]}; }
 }
 const dependencies = {'../broker':Broker, '../retry':()=>async fn=>fn(e=>{throw e;},0,0), '../utils/shuffle':x=>x, '../utils/arrayDiff':(a,b)=>a.filter(x=>!b.includes(x)), '../errors':{KafkaJSBrokerNotFound:class extends Error {},KafkaJSProtocolError:class extends Error {}}};
 const m={exports:{}};
 vm.runInNewContext(fs.readFileSync(source,'utf8'), {module:m, require:id=>{assert.ok(Object.hasOwn(dependencies,id));return dependencies[id];}}, {filename:source,timeout:1000});
 const logger={namespace:()=>logger,error:()=>{}};
 return new m.exports({logger, retry:{retries:0}, connectionPoolBuilder:{build:async (o={host:'kafka',port:9092})=>o}});
}
const rows=(high='4')=>[0,1].map(partition=>({partition,offset:high,high,low:'0'}));
function fixture(mode) {
 const p=pool(),calls=[]; let reads=0;
 const admin={
  connect:async()=>{calls.push('connect');await p.connect();},
  fetchTopicOffsets:async()=>{calls.push('fetchTopicOffsets');reads++;if(mode==='throw')throw new Error('bootstrap unavailable');await p.refreshMetadata(['orders.events']);return mode==='invalid'?rows('unknown'):rows(reads===1?'4':'5');},
  describeGroups:async ids=>{calls.push('describeGroups');await p.withBroker(async()=>true);return {groups:ids.map(groupId=>({groupId,state:'Empty',protocol:'',protocolType:'',members:[]}))};},
  fetchOffsets:async options=>{calls.push('fetchOffsets');assert.equal(options.resolveOffsets,false);assert.deepEqual(options.topics,['orders.events']);return [{topic:'orders.events',partitions:[0,1].map(partition=>({partition,offset:'3'}))}];},
  disconnect:async()=>{calls.push('disconnect');await p.disconnect();}
 };return {admin,calls};
}
test('archived real BrokerPool connect leaves empty; real metadata populates withBroker',async()=>{
 const p=pool();await p.connect();assert.deepEqual(Object.keys(p.brokers),[]);
 await assert.rejects(p.withBroker(async()=>true),/No brokers in the broker pool/);
 await p.refreshMetadata(['orders.events']);assert.deepEqual(Object.keys(p.brokers),['1']);assert.equal(await p.withBroker(async()=>true),true);await p.disconnect();
});
test('bootstrap precedes describe and separate fresh end read supplies lag',async()=>{
 const f=fixture();const r=await require(target).probe({...f,decode:()=>{throw Error('unexpected');}});
 assert.equal(r.status,'ok',JSON.stringify(r));assert.equal(r.diagnosticOnly,true);
 assert.deepEqual(f.calls,['connect','fetchTopicOffsets','describeGroups','fetchOffsets','fetchOffsets','fetchOffsets','fetchTopicOffsets','disconnect']);
 assert.equal(r.raw.bootstrap[0].high,'4');assert.equal(r.raw.end[0].high,'5');assert.equal(r.groups[0].offsets[0].lag,2);
});
for(const mode of ['throw','invalid'])test('bootstrap fail closed '+mode,async()=>{
 const f=fixture(mode);const r=await require(target).probe({...f,decode:()=>{}});
 assert.equal(r.status,'error');assert.equal(r.phase,'bootstrapTopicOffsets');assert.deepEqual(f.calls,['connect','fetchTopicOffsets','disconnect']);
 if(mode==='invalid')assert.equal(r.raw.bootstrap[0].high,'unknown');else assert.equal(r.error.message,'bootstrap unavailable');
});
// Actual Node stdin subprocess, no KafkaJS installation or network. API shim obeys pool prerequisite.
const shim=`const Module=require('node:module');const original=Module.prototype.require;let ready=false;const calls=[];const rows=[0,1].map(partition=>({partition,offset:'4',high:'4',low:'0'}));Module.prototype.require=function(id){if(id!=='kafkajs')return original.apply(this,arguments);calls.push('require');return {Kafka:class{constructor(c){if(c.retry.retries!==0||c.requestTimeout!==2000||c.connectionTimeout!==1000)throw Error('config');}admin(o){if(o.retry.retries!==0)throw Error('retry');return {connect:async()=>calls.push('connect'),fetchTopicOffsets:async()=>{calls.push('fetchTopicOffsets');if(process.env.BOOT_FAIL==='1')throw Error('bootstrap unavailable');ready=true;return rows;},describeGroups:async ids=>{calls.push('describeGroups');if(!ready)throw Error('No brokers in the broker pool');return {groups:ids.map(groupId=>({groupId,state:'Empty',protocol:'',protocolType:'',members:[]}))};},fetchOffsets:async o=>{calls.push('fetchOffsets');if(o.resolveOffsets!==false)throw Error('mutation');return [{topic:'orders.events',partitions:rows.map(p=>({partition:p.partition,offset:'3'}))}];},disconnect:async()=>{calls.push('disconnect');process.stderr.write(JSON.stringify(calls)+'\\n');}};}},AssignerProtocol:{MemberAssignment:{decode:()=>{throw Error('decode');}}}};};`;
test('stdin success, bootstrap failure, inert import and main gate',()=>{
 const evidence=path.join(__dirname,'bootstrap03-test-evidence');fs.mkdirSync(evidence,{recursive:true});
 const runId=process.env.RECEIPT_ID || String(Date.now());
 for(const mode of ['success','failure','import','main']){
  const code=mode==='import'?`process.argv[1]='-';require(${JSON.stringify(target)});process.stdout.write('inert');`:mode==='main'?`const m=new Module(${JSON.stringify(target)});m.filename=${JSON.stringify(target)};m.paths=module.paths;require.main=m;process.mainModule=m;process.argv=[process.execPath,m.filename,'kafka:9092'];m._compile(require('node:fs').readFileSync(m.filename,'utf8'),m.filename);`:fs.readFileSync(target,'utf8');
  const argv=[process.execPath,'-','kafka:9092'],utcStart=new Date().toISOString();
  const r=spawnSync(argv[0],argv.slice(1),{input:shim+'\n'+code,encoding:'utf8',timeout:3000,env:{...process.env,BOOT_FAIL:mode==='failure'?'1':'0'}});
  fs.writeFileSync(path.join(evidence,`${runId}-${mode}.json`),JSON.stringify({argv,utcStart,utcEnd:new Date().toISOString(),stdout:r.stdout,stderr:r.stderr,status:r.status,signal:r.signal,error:r.error?String(r.error):null},null,2)+'\n',{flag:'wx'});
  assert.ifError(r.error);assert.equal(r.status,mode==='failure'?1:0);
  if(mode==='import'){assert.equal(r.stdout,'inert');assert.equal(r.stderr,'');continue;}
  const result=JSON.parse(r.stdout);assert.equal(result.status,mode==='failure'?'error':'ok');
  const calls=JSON.parse(r.stderr);assert.equal(calls.at(-1),'disconnect');
  if(mode==='failure'){assert.equal(result.phase,'bootstrapTopicOffsets');assert.deepEqual(calls,['require','connect','fetchTopicOffsets','disconnect']);}
  else assert.deepEqual(calls,['require','connect','fetchTopicOffsets','describeGroups','fetchOffsets','fetchOffsets','fetchOffsets','fetchTopicOffsets','disconnect']);
 }
});
