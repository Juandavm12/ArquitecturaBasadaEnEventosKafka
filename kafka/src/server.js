import { Kafka, Partitioners } from 'kafkajs';
import { hostname } from 'node:os';
import { once } from 'node:events';
import { createApp } from './app.js';

const topic = process.env.KAFKA_TOPIC || 'orders.events';
const log = entry => console.log(JSON.stringify(entry));
const report = (phase, error) => log({ date: new Date().toISOString(),
  service: 'orders-api', hostname: hostname(), topic, phase, error: error.message });
const kafka = new Kafka({ clientId: 'orders-api',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
  connectionTimeout: 3000, requestTimeout: 10000, enforceRequestTimeout: true,
  retry: { retries: 2, initialRetryTime: 200, maxRetryTime: 1000 } });
const admin = kafka.admin();
const producer = kafka.producer({ allowAutoTopicCreation: false,
  createPartitioner: Partitioners.DefaultPartitioner });
let server, stopping = false;
async function checkBroker() {
  const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
  const current = metadata.topics.find(item => item.name === topic);
  if (!current || current.partitions.length < 2) throw new Error('Topic requiere al menos 2 particiones');
}
async function shutdown(code) {
  if (stopping) return;
  stopping = true;
  const deadline = setTimeout(() => process.exit(1), 12000);
  try {
    if (server) await new Promise((resolve, reject) => {
      server.close(error => error ? reject(error) : resolve());
      server.closeIdleConnections();
    });
    const results = await Promise.allSettled([producer.disconnect(), admin.disconnect()]);
    for (const result of results) if (result.status === 'rejected') {
      report('shutdown', result.reason);
      code = 1;
    }
  } catch (error) { report('shutdown', error); code = 1; }
  clearTimeout(deadline);
  process.exit(code);
}
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => shutdown(0));
try {
  await admin.connect();
  await admin.createTopics({ waitForLeaders: true, timeout: 10000,
    topics: [{ topic, numPartitions: 2, replicationFactor: 1 }] });
  await checkBroker();
  await producer.connect();
  if (!stopping) {
    server = createApp({ producer, topic, checkBroker, log }).listen(3000, '0.0.0.0');
    await once(server, 'listening');
  }
} catch (error) {
  report('startup', error);
  await shutdown(1);
}
