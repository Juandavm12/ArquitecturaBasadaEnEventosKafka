const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const target = path.join(__dirname, '..', 'CP04.postman_collection.json');
function parse(text) {
  try { return JSON.parse(text); } catch (error) { throw new Error(`JSON inválido: ${error.message}`); }
}
const c = parse(fs.readFileSync(target, 'utf8'));
assert.equal(c.item.length, 1);
assert.equal(c.auth.type, 'noauth');
assert.equal(c.event, undefined);
const item = c.item[0];
assert.equal(item.item, undefined);
assert.equal(item.request.method, 'POST');
assert.equal(item.request.url, 'http://localhost:3000/orders');
assert.equal(item.request.auth.type, 'noauth');
assert.deepEqual(item.request.header, [{ key: 'Content-Type', value: 'application/json' }]);
assert.equal(item.request.body.mode, 'raw');
assert.deepEqual(parse(item.request.body.raw), {
  customerName: 'Pedro', product: 'Libro de arquitectura', quantity: 1, unitPrice: 85000
});
assert.equal(item.event.length, 1);
assert.equal(item.event[0].listen, 'test');
const source = item.event[0].script.exec.join('\n');
assert.doesNotMatch(source, /sendRequest|setNextRequest|fetch|require|await|XMLHttpRequest|\b(for|while)\s*\(/);
const script = new vm.Script(source);
const good = { status: 'accepted', message: 'Pedido recibido y evento OrderCreated publicado en Kafka',
  orderId: 'ORD-f2035454-c380-4e9f-b682-b48ef72ca568' };
function run(code, body) {
  const results = [], logs = [];
  const pm = {
    request: { method: 'POST', url: { toString: () => item.request.url }, body: item.request.body },
    response: { code, responseTime: 123, text: () => body, json: () => parse(body) },
    test: (name, fn) => { try { fn(); results.push([name, true]); } catch { results.push([name, false]); } },
    expect: actual => ({ to: { eql: expected => assert.deepEqual(actual, expected),
      match: regex => { assert.equal(typeof actual, 'string'); assert.match(actual, regex); } } })
  };
  script.runInNewContext({ pm, console: { log: (...args) => logs.push(args.join(' ')) } }, { timeout: 1000 });
  assert.equal(results.length, 5);
  assert(logs.some(x => x.includes('REQUEST_METHOD POST')));
  assert(logs.some(x => x.includes('REQUEST_URL http://localhost:3000/orders')));
  assert(logs.some(x => x.includes('REQUEST_BODY') && x.includes('Pedro')));
  assert(logs.some(x => x.includes(`RESPONSE_CODE ${code}`)));
  assert(logs.some(x => x.includes('RESPONSE_BODY') && x.includes(body)));
  assert(logs.some(x => x.includes('ORDER_ID')));
  assert(logs.some(x => x.includes('RESPONSE_TIME_MS 123')));
  return results.filter(x => !x[1]).map(x => x[0]);
}
assert.deepEqual(run(202, JSON.stringify(good)), []);
assert.deepEqual(run(503, JSON.stringify(good)), ['HTTP 202']);
assert.equal(run(202, 'not JSON').length, 4);
assert.deepEqual(run(202, JSON.stringify({ ...good, message: 'wrong' })), ['Mensaje exacto']);
assert.deepEqual(run(202, JSON.stringify({ ...good, orderId: 'ORD-invalid' })), ['orderId ORD-UUID']);
assert.deepEqual(run(202, JSON.stringify({ ...good, status: 'error' })), ['Status accepted']);
console.log('PASS: estructura de un POST; VM 202/503/JSON inválido/mensaje/ID/status; evidencia explícita. Sin HTTP.');
