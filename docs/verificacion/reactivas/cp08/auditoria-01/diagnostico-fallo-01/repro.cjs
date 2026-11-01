'use strict';
// Offline only. Never executes argv from evidence; no Docker or source writes.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),vm=require('node:vm');
const out=__dirname, input=path.join(out,'../lectura-runtime');
const read=n=>fs.readFileSync(path.join(input,n));
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
function parse(text){try{return JSON.parse(text);}catch(cause){throw new Error('Offline fixture parse failed',{cause});}}
const raw=read('03-stdout.txt'), command=parse(read('03-command.json'));
const local=parse(read('local-source-hashes.json'));
const baseline=parse(read('baseline.json'));
const original=String.raw`const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{spawnSync}=require('node:child_process');
let sequence=0;
function put(name,data){fs.writeFileSync(path.join(out,name),data,{flag:'wx'});}
function json(name,data){put(name,JSON.stringify(data,null,2)+'\n');}
function run(argv){const prefix=String(++sequence).padStart(2,'0'),startUTC=new Date().toISOString();const r=spawnSync(argv[0],argv.slice(1),{encoding:'utf8',timeout:15000,maxBuffer:10*1024*1024});put(prefix+'-stdout.txt',r.stdout||'');put(prefix+'-stderr.txt',r.stderr||'');json(prefix+'-command.json',{argv,cwd:process.cwd(),startUTC,endUTC:new Date().toISOString(),exitCode:r.status,signal:r.signal,error:r.error?.message||null});assert.equal(r.status,0,r.error?.message||r.stderr);return r.stdout;}`;
function errorReceipt(e,seen=new Set()){
 if(e===null||typeof e!=='object')return {type:typeof e,value:String(e)};
 if(seen.has(e))return {circular:true};seen.add(e);
 const r={};for(const k of Object.getOwnPropertyNames(e)){try{const v=e[k];r[k]=typeof v==='bigint'?String(v):v&&typeof v==='object'?errorReceipt(v,seen):v;}catch(x){r[k]={unreadable:String(x)};}}
 return r;
}
const captures=[];
// In-memory filesystem substitutes only archive writes; fixture bytes remain unchanged.
const fakeFs={writeFileSync:(p,d,o)=>{assert.equal(o.flag,'wx');captures.push({name:path.basename(p),bytes:Buffer.byteLength(d)});}};
const sandbox={out:'/offline',process:{cwd:()=>'/offline'},argv:command.argv,require:n=>n==='node:fs'?fakeFs:n==='node:child_process'?{spawnSync:()=>({stdout:raw.toString('utf8'),stderr:'',status:0,signal:null})}:require(n)};
vm.runInNewContext(original+'\nresult=JSON.parse(run(argv));',sandbox,{timeout:1000});
const parsed=parse(raw.toString('utf8'));
assert.deepEqual(parse(JSON.stringify(sandbox.result)),parsed);
assert.deepEqual(parsed,local);
assert.equal(command.exitCode,0);assert.equal(command.signal,null);assert.equal(command.error,null);assert.equal(read('03-stderr.txt').length,0);
const synthetic=new Error('Invalid remote hash JSON',{cause:new SyntaxError('synthetic, not historical')});
assert.equal(errorReceipt(synthetic).cause.message,'synthetic, not historical');
const report={node:process.version,fixture:{bytes:raw.length,sha256:hash(raw),firstBytes:raw.subarray(0,8).toString('hex'),lastBytes:raw.subarray(-8).toString('hex'),keys:Object.keys(parsed),matchesLocal:true},command,originalWrapperReplay:'PASS with in-memory archive and fixture spawn result; not original collector PASS',captures,baseline:{keys:Object.keys(baseline),originalCount:Object.keys(baseline.originals||{}).length},inputDigests:Object.fromEntries(['baseline.json','local-source-hashes.json','01-command.json','02-command.json','03-command.json','03-stdout.txt','03-stderr.txt'].map(n=>[n,hash(read(n))])),historicalCause:'unknown',syntheticCauseReceipt:errorReceipt(synthetic)};
fs.writeFileSync(path.join(out,'offline-result.json'),JSON.stringify(report,null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({matchesLocal:true,bytes:raw.length,originalCount:report.baseline.originalCount,wrapperReplay:'PASS',historicalCause:'unknown'}));
module.exports={errorReceipt};
