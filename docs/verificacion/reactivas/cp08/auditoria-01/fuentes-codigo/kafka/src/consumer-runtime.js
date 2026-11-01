import { getConfig, createSimulatedAction, createMessageHandler } from './consumer.js';

export function readSettings(env) {
  const role = env.CONSUMER_ROLE;
  const config = getConfig(role);
  const raw = env.PROCESSING_DELAY_MS ?? '0';
  if (!/^\d+$/.test(raw) || Number(raw) > 10000) {
    throw new TypeError('PROCESSING_DELAY_MS must be an integer in 0..10000');
  }
  return { ...config, role, delayMs: Number(raw),
    broker: env.KAFKA_BROKER || 'kafka:9092', topic: env.KAFKA_TOPIC || 'orders.events' };
}

export function createRuntime({ consumer, role, instanceId, logger, marker, onFatal,
  delayMs = 0, topic = 'orders.events',
  deadline = callback => {
    const timer = setTimeout(callback, 12000);
    return () => clearTimeout(timer);
  } }) {
  const config = getConfig(role);
  let closing, connecting, failed = false, stopping = false;
  const log = record => logger({ service: config.service, groupId: config.groupId,
    instanceId, ...record });
  function shutdown() {
    if (closing) return closing;
    stopping = true;
    closing = Promise.resolve().then(async () => {
      const cancel = deadline(() => {
        log({ phase: 'shutdown-timeout' });
        onFatal(1);
      });
      try {
        marker(false);
        // A signal during connect must not leave a late connection behind.
        try { await connecting; } catch { /* start reports the bootstrap error */ }
        try { await consumer.stop(); } finally { await consumer.disconnect(); }
        log({ phase: 'shutdown-complete' });
      } catch (error) {
        failed = true;
        log({ phase: 'cleanup-error', error: error.message });
        throw error;
      } finally {
        cancel();
        if (failed) onFatal(1);
      }
    });
    return closing;
  }
  function fatal(error) {
    failed = true;
    marker(false);
    log({ phase: 'fatal', error: error?.message || String(error) });
    return shutdown();
  }
  async function start() {
    if (stopping) return;
    try {
      marker(false);
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
        if (stopping || failed) return;
        try {
          marker(true);
          log({ phase: 'group-join', assignment: payload.memberAssignment });
        } catch (error) { void fatal(error).catch(() => {}); }
      });
      consumer.on(consumer.events.DISCONNECT, () => { marker(false); });
      consumer.on(consumer.events.CRASH, ({ payload }) => {
        void fatal(payload.error).catch(() => {});
      });
      connecting = consumer.connect();
      await connecting;
      if (stopping) return;
      await consumer.subscribe({ topic, fromBeginning: true });
      if (stopping) return;
      await consumer.run({ autoCommit: false,
        eachMessage: createMessageHandler({ role, instanceId,
          action: createSimulatedAction({ role, delayMs }), logger: log,
          commitOffsets: offsets => consumer.commitOffsets(offsets) }) });
    } catch (error) {
      await fatal(error);
      throw error;
    }
  }
  return { start, shutdown };
}
