import express from 'express';
import { hostname } from 'node:os';
import { createOrderEvent } from './order-event.js';

export function createApp({ producer, topic, checkBroker, log }) {
  const app = express();
  app.disable('x-powered-by');
  const record = fields => log({ date: new Date().toISOString(),
    service: 'orders-api', hostname: hostname(), ...fields });
  app.use(express.json({ limit: '32kb' }));
  app.get('/health', async (_req, res) => {
    try {
      await checkBroker();
      res.json({ status: 'ok' });
    } catch (error) {
      record({ topic, error: error.message });
      res.status(503).json({ error: 'Kafka no disponible' });
    }
  });
  app.post('/orders', async (req, res) => {
    let event;
    try { event = createOrderEvent(req.body); }
    catch (error) { return res.status(400).json({ error: error.message }); }
    const ids = { orderId: event.data.orderId, eventId: event.eventId,
      correlationId: event.correlationId, topic };
    try {
      const metadata = await producer.send({ topic, acks: -1, timeout: 10000,
        messages: [{ key: ids.orderId, value: JSON.stringify(event),
          headers: { correlationId: ids.correlationId, eventId: ids.eventId } }] });
      for (const { partition, baseOffset } of metadata) record({ ...ids, partition, baseOffset });
      res.status(202).json({ status: 'accepted',
        message: 'Pedido recibido y evento OrderCreated publicado en Kafka', orderId: ids.orderId });
    } catch (error) {
      record({ ...ids, error: error.message });
      res.status(503).json({ error: 'Kafka no disponible' });
    }
  });
  app.use((error, _req, res, _next) => {
    const status = error.type === 'entity.too.large' ? 413 : 400;
    res.status(status).json({ error: status === 413 ? 'JSON excede 32kb' : 'JSON inválido' });
  });
  return app;
}
