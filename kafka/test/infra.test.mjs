import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { getConfig } from '../src/consumer.js';

const cwd = fileURLToPath(new URL('../', import.meta.url));
function config(overrides = {}) {
  const args = ['compose', '-f', 'docker-compose.yml', 'config', '--format', 'json'];
  const result = spawnSync('docker', args, {
    cwd, encoding: 'utf8', env: { ...process.env, ...overrides },
  });
  console.log(JSON.stringify({ command: ['docker', ...args], overrides,
    code: result.status, stdout: result.stdout, stderr: result.stderr }));
  assert.equal(result.status, 0, result.stderr || result.error?.message);
  return JSON.parse(result.stdout).services;
}

test('contrato estático broker + UI + API; no requiere daemon', () => {
  const services = config({ KAFKA_EXTERNAL_PORT: '29092', KAFKA_UI_PORT: '8080', ORDERS_API_PORT: '3000' });
  assert.deepEqual(Object.keys(services).sort(), ['billing-consumer', 'inventory-consumer',
    'kafka', 'kafka-ui', 'notification-consumer', 'orders-api']);
  const groups = new Set();
  for (const role of ['inventory', 'billing', 'notification']) {
    const service = services[`${role}-consumer`];
    groups.add(getConfig(role).groupId);
    assert.equal(service.build.context, resolve(cwd));
    assert.deepEqual(service.command, ['node', 'src/consumer-server.js']);
    assert.equal(service.environment.CONSUMER_ROLE, role);
    assert.equal(service.environment.KAFKA_BROKER, 'kafka:9092');
    assert.equal(service.environment.KAFKA_TOPIC, 'orders.events');
    assert.equal(service.environment.PROCESSING_DELAY_MS, '0');
    assert.equal(service.depends_on.kafka.condition, 'service_healthy');
    assert.equal(service.restart, 'on-failure');
    assert.equal(service.stop_grace_period, '15s');
    assert.equal(service.container_name, undefined);
    assert.equal(service.ports, undefined);
    assert.deepEqual(service.healthcheck.test, ['CMD', 'test', '-f', '/tmp/consumer-ready']);
    assert.equal(service.healthcheck.interval, '10s');
    assert.equal(service.healthcheck.timeout, '5s');
    assert.equal(service.healthcheck.retries, 6);
    assert.equal(service.healthcheck.start_period, '20s');
  }
  assert.equal(groups.size, 3);
  const api = services['orders-api'];
  assert.equal(api.container_name, undefined);
  assert.equal(api.build.context, resolve(cwd));
  assert.equal(api.environment.KAFKA_BROKER, 'kafka:9092');
  assert.equal(api.environment.KAFKA_TOPIC, 'orders.events');
  assert.equal(api.depends_on.kafka.condition, 'service_healthy');
  assert.equal(api.ports[0].host_ip, '127.0.0.1');
  assert.equal(api.ports[0].target, 3000);
  assert.equal(api.ports[0].published, '3000');
  assert.equal(api.stop_grace_period, '15s');
  assert.deepEqual(api.healthcheck.test.slice(0, 3), ['CMD', 'node', '-e']);
  assert.match(api.healthcheck.test[3], /127\.0\.0\.1:3000\/health/);
  assert.match(api.healthcheck.test[3], /r\.ok/);
  const broker = services.kafka;
  const ui = services['kafka-ui'];
  const env = broker.environment;
  assert.equal(broker.image, 'apache/kafka:3.9.1');
  assert.equal(ui.image, 'provectuslabs/kafka-ui:v0.7.2');
  for (const service of [broker, ui]) assert.equal(service.container_name, undefined);
  assert.equal(env.KAFKA_NODE_ID, '1');
  assert.equal(env.KAFKA_PROCESS_ROLES, 'broker,controller');
  assert.equal(env.KAFKA_CONTROLLER_QUORUM_VOTERS, '1@kafka:9093');
  assert.equal(env.KAFKA_LISTENERS, 'INTERNAL://:9092,EXTERNAL://:29092,CONTROLLER://:9093');
  assert.equal(env.KAFKA_ADVERTISED_LISTENERS, 'INTERNAL://kafka:9092,EXTERNAL://localhost:29092');
  assert.equal(env.KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, 'INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT');
  assert.equal(env.KAFKA_INTER_BROKER_LISTENER_NAME, 'INTERNAL');
  assert.equal(env.KAFKA_CONTROLLER_LISTENER_NAMES, 'CONTROLLER');
  assert.equal(env.KAFKA_NUM_PARTITIONS, '2');
  assert.equal(env.KAFKA_AUTO_CREATE_TOPICS_ENABLE, 'false');
  for (const key of ['OFFSETS_TOPIC_REPLICATION_FACTOR', 'TRANSACTION_STATE_LOG_REPLICATION_FACTOR', 'TRANSACTION_STATE_LOG_MIN_ISR', 'MIN_INSYNC_REPLICAS']) {
    assert.equal(env[`KAFKA_${key}`], '1');
  }
  assert.equal(env.KAFKA_LOG_RETENTION_HOURS, '168');
  assert.equal(env.KAFKA_HEAP_OPTS, '-Xms512m -Xmx512m');
  assert.match(env.CLUSTER_ID, /^[A-Za-z0-9_-]{22}$/);
  assert.ok(!Object.keys(env).some(key => key.startsWith('KAFKA_CFG_')));
  assert.equal(env.KAFKA_LOG_DIRS, '/var/lib/kafka/data');
  assert.ok(broker.volumes.some(v => v.type === 'volume' && v.source === 'kafka-data' && v.target === env.KAFKA_LOG_DIRS));
  assert.deepEqual(broker.healthcheck.test, ['CMD', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:9092', '--list']);
  assert.equal(broker.healthcheck.interval, '10s');
  assert.equal(broker.healthcheck.timeout, '10s');
  assert.equal(broker.healthcheck.retries, 12);
  assert.equal(broker.healthcheck.start_period, '20s');
  assert.equal(ui.depends_on.kafka.condition, 'service_healthy');
  assert.equal(ui.environment.KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS, 'kafka:9092');
  assert.equal(ui.environment.KAFKA_CLUSTERS_0_READONLY, 'true');
  assert.equal(ui.environment.DYNAMIC_CONFIG_ENABLED, 'false');
  assert.equal(ui.environment.JAVA_OPTS, '-Xms256m -Xmx256m');
  for (const [service, target, published] of [[broker, 29092, '29092'], [ui, 8080, '8080']]) {
    assert.equal(service.ports.length, 1);
    assert.equal(service.ports[0].host_ip, '127.0.0.1');
    assert.equal(service.ports[0].target, target);
    assert.equal(service.ports[0].published, published);
  }
});

test('puertos alternativos conservan listener interno y anuncio externo coherentes', () => {
  const services = config({ KAFKA_EXTERNAL_PORT: '39092', KAFKA_UI_PORT: '18080', ORDERS_API_PORT: '4000' });
  assert.equal(services['orders-api'].ports[0].published, '4000');
  assert.equal(services['orders-api'].ports[0].target, 3000);
  assert.equal(services.kafka.ports[0].published, '39092');
  assert.equal(services.kafka.ports[0].target, 29092);
  assert.equal(services.kafka.environment.KAFKA_ADVERTISED_LISTENERS,
    'INTERNAL://kafka:9092,EXTERNAL://localhost:39092');
  assert.equal(services['kafka-ui'].ports[0].published, '18080');
  for (const name of ['kafka', 'kafka-ui', 'orders-api']) {
    assert.equal(services[name].ports[0].host_ip, '127.0.0.1');
  }
});
