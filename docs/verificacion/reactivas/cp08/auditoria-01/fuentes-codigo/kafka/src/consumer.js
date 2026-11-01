import { setTimeout as sleepDefault } from 'node:timers/promises';

export const ROLE_CONFIG = Object.freeze({
  inventory: Object.freeze({ service: 'inventory-consumer', groupId: 'inventory-service-group',
    action: 'simulate-stock-reservation' }),
  billing: Object.freeze({ service: 'billing-consumer', groupId: 'billing-service-group',
    action: 'simulate-invoice-charge' }),
  notification: Object.freeze({ service: 'notification-consumer', groupId: 'notification-service-group',
    action: 'simulate-notification-send' }),
});

export function getConfig(role) {
  if (!Object.hasOwn(ROLE_CONFIG, role)) throw new TypeError('Unknown role');
  return ROLE_CONFIG[role];
}

export function createSimulatedAction({ role, delayMs = 0, sleep = sleepDefault }) {
  const { action } = getConfig(role);
  if (!Number.isFinite(delayMs) || delayMs < 0) {
    throw new TypeError('delayMs must be finite and nonnegative');
  }
  return async (event) => {
    if (delayMs > 0) await sleep(delayMs);
    return { action, orderId: event.data.orderId };
  };
}

function nonempty(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

function decode(value) {
  if (!Buffer.isBuffer(value)) throw new TypeError('message.value must be a Buffer');
  let event;
  try {
    event = JSON.parse(value.toString('utf8'));
  } catch (cause) {
    throw new SyntaxError('Invalid message JSON', { cause });
  }
  if (!event || typeof event !== 'object' || Array.isArray(event)
      || event.eventType !== 'OrderCreated' || event.eventVersion !== '1.0'
      || !nonempty(event.eventId) || !nonempty(event.correlationId)
      || !nonempty(event.source) || !nonempty(event.occurredAt)
      || !Number.isFinite(Date.parse(event.occurredAt))
      || !event.data || typeof event.data !== 'object' || Array.isArray(event.data)
      || !nonempty(event.data.orderId)) {
    throw new TypeError('Invalid OrderCreated v1.0 envelope');
  }
  return event;
}

/** Returns a KafkaJS eachMessage-compatible callback; no broker dependency. */
export function createMessageHandler({ role, instanceId, action, logger, commitOffsets }) {
  const config = getConfig(role);
  if (!nonempty(instanceId)) throw new TypeError('instanceId must be nonempty');
  for (const dependency of [action, logger, commitOffsets]) {
    if (typeof dependency !== 'function') throw new TypeError('Dependencies must be functions');
  }
  return async ({ topic, partition, message, heartbeat }) => {
    const { offset } = message;
    if (typeof offset !== 'string' || !/^\d+$/.test(offset)) {
      throw new TypeError('Invalid decimal nonnegative offset');
    }
    const nextOffset = (BigInt(offset) + 1n).toString();
    const event = decode(message.value);
    const trace = {
      service: config.service, groupId: config.groupId, instanceId,
      orderId: event.data.orderId, eventId: event.eventId,
      correlationId: event.correlationId, topic, partition, offset,
    };
    await logger({ phase: 'received', action: config.action, trace, event });
    await heartbeat();
    const result = await action(event);
    await heartbeat();
    await logger({ phase: 'processed', action: config.action, trace, result });
    await commitOffsets([{ topic, partition, offset: nextOffset }]);
    await logger({ phase: 'offset-committed', action: config.action, trace });
  };
}
