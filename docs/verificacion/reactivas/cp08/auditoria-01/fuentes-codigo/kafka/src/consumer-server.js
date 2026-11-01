import { hostname } from 'node:os';
import { writeFileSync, rmSync } from 'node:fs';
import { Kafka } from 'kafkajs';
import { createRuntime, readSettings } from './consumer-runtime.js';

const instanceId = hostname();
const logger = record => console.log(JSON.stringify({ date: new Date().toISOString(),
  hostname: instanceId, ...record }));
const marker = ready => {
  if (ready) writeFileSync('/tmp/consumer-ready', 'joined\n');
  else rmSync('/tmp/consumer-ready', { force: true });
};
try {
  marker(false);
  const settings = readSettings(process.env);
  const kafka = new Kafka({ clientId: `${settings.service}-${instanceId}`,
    brokers: [settings.broker], retry: { restartOnFailure: async () => false } });
  const consumer = kafka.consumer({ groupId: settings.groupId });
  const runtime = createRuntime({ ...settings, consumer, instanceId, logger, marker,
    onFatal: code => process.exit(code) });
  for (const signal of ['SIGTERM', 'SIGINT']) {
    process.on(signal, () => { void runtime.shutdown().catch(() => {}); });
  }
  await runtime.start();
} catch (error) {
  logger({ phase: 'entry-error', error: error.message });
  process.exit(1);
}
