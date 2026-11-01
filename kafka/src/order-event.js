import { randomUUID } from 'node:crypto';

/** Construye el hecho de dominio definido en el anexo 2; no publica en Kafka. */
export function createOrderEvent(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    throw new TypeError('El pedido debe ser un objeto JSON.');
  }
  const { customerName, product, quantity, unitPrice } = input;
  for (const [field, value] of Object.entries({ customerName, product })) {
    if (typeof value !== 'string' || value.trim().length === 0) {
      throw new TypeError(`${field} debe ser texto no vacío.`);
    }
  }
  if (!Number.isSafeInteger(quantity) || quantity <= 0) {
    throw new TypeError('quantity debe ser un entero positivo seguro.');
  }
  if (typeof unitPrice !== 'number' || !Number.isFinite(unitPrice) || unitPrice < 0) {
    throw new TypeError('unitPrice debe ser un número finito no negativo.');
  }
  const total = quantity * unitPrice;
  if (!Number.isFinite(total)) {
    throw new TypeError('El total debe ser finito.');
  }
  return {
    eventId: randomUUID(),
    eventType: 'OrderCreated',
    eventVersion: '1.0',
    occurredAt: new Date().toISOString(),
    source: 'orders-api',
    correlationId: randomUUID(),
    data: {
      orderId: `ORD-${randomUUID()}`,
      customerName,
      product,
      quantity,
      unitPrice,
      total,
    },
  };
}
