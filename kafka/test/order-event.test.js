import assert from 'node:assert/strict';
import test from 'node:test';
import { createOrderEvent } from '../src/order-event.js';

const input = {
  customerName: 'Pedro',
  product: 'Libro de arquitectura',
  quantity: 1,
  unitPrice: 85000,
};

test('construye el contrato OrderCreated del anexo 2 sin modificar la entrada', () => {
  const original = structuredClone(input);
  const event = createOrderEvent(input);
  assert.equal(event.eventType, 'OrderCreated');
  assert.equal(event.eventVersion, '1.0');
  assert.equal(event.source, 'orders-api');
  assert.match(event.eventId, /^[0-9a-f-]{36}$/);
  assert.match(event.correlationId, /^[0-9a-f-]{36}$/);
  assert.match(event.data.orderId, /^ORD-[0-9a-f-]{36}$/);
  assert.equal(new Date(event.occurredAt).toISOString(), event.occurredAt);
  assert.deepEqual(event.data, { ...input, orderId: event.data.orderId, total: 85000 });
  assert.deepEqual(input, original);
});

test('genera identificadores diferentes en pedidos consecutivos', () => {
  const first = createOrderEvent(input);
  const second = createOrderEvent(input);
  for (const key of ['eventId', 'correlationId']) {
    assert.notEqual(first[key], second[key]);
  }
  assert.notEqual(first.data.orderId, second.data.orderId);
});

test('calcula total y conserva únicamente campos del contrato', () => {
  const event = createOrderEvent({ ...input, quantity: 3, extra: 'no publicar' });
  assert.equal(event.data.total, 255000);
  assert.equal(Object.hasOwn(event.data, 'extra'), false);
});

for (const invalid of [
  null, [], 'texto', {},
  { ...input, customerName: '' },
  { ...input, customerName: '   ' },
  { ...input, product: 12 },
  { ...input, quantity: 0 },
  { ...input, quantity: -1 },
  { ...input, quantity: 1.5 },
  { ...input, quantity: '1' },
  { ...input, quantity: Number.MAX_SAFE_INTEGER + 1 },
  { ...input, unitPrice: -1 },
  { ...input, unitPrice: '85000' },
  { ...input, unitPrice: Infinity },
  { ...input, unitPrice: NaN },
  { ...input, unitPrice: Number.MAX_VALUE, quantity: 2 },
]) {
  test(`rechaza datos inválidos: ${JSON.stringify(invalid)}`, () => {
    assert.throws(() => createOrderEvent(invalid), { name: 'TypeError' });
  });
}

test('admite precio cero: no se impone una regla comercial ausente en la guía', () => {
  assert.equal(createOrderEvent({ ...input, unitPrice: 0 }).data.total, 0);
});
