# API y consumidores: contrato actual

## API/productor

`POST /orders` acepta `customerName` y `product` no vacíos, `quantity` entero positivo seguro y `unitPrice` finito no negativo; el total debe ser finito. Máximo JSON: 32 kb. Entrada inválida: 400; exceso de tamaño: 413; fallo Kafka: 503.

El 202 incluye `status: accepted`, `orderId` y `Pedido recibido y evento OrderCreated publicado en Kafka`. Solo se envía tras confirmar `producer.send` con acks -1 y timeout 10000 ms; no espera consumidores ni certifica acciones de negocio. Un 503 puede ser ambiguo: Kafka pudo recibir el mensaje. Repetir el POST crea nuevos IDs y puede duplicar un pedido lógico.

Evento `OrderCreated` versión `1.0`: `eventId`, `occurredAt`, `source: orders-api`, `correlationId` y `data` con pedido y total. Key Kafka: `orderId`; headers: `eventId` y `correlationId`. Los logs registran IDs, topic, partición y `baseOffset` devueltos por Kafka.

Al iniciar, la API crea idempotentemente `orders.events` con dos particiones y RF1 y conecta el productor antes de escuchar. Un topic existente con menos de dos particiones produce fallo; `GET /health` consulta metadata sin publicar. Los eventos se almacenan en el volumen de Kafka; no en una base de datos de la API.

## Tres roles y tres grupos

| Rol | Servicio | Grupo | Acción simulada |
| --- | --- | --- | --- |
| inventory | inventory-consumer | inventory-service-group | Reserva de inventario |
| billing | billing-consumer | billing-service-group | Factura/cobro |
| notification | notification-consumer | notification-service-group | Notificación |

Los tres grupos reciben el flujo de forma independiente. Réplicas del mismo rol comparten grupo y reparten particiones; dos particiones limitan el paralelismo útil por grupo. No hay DB, cobro ni email real.

`consumer.js` expone configuración inmutable, acción simulada y handler inyectable; `consumer-runtime.js` conecta KafkaJS con `autoCommit: false` y `fromBeginning: true`; `consumer-server.js` configura el proceso. No se han movido fuentes por estética.

El handler valida el envelope (no repite todas las reglas comerciales), registra `received`, espera heartbeat, ejecuta la acción, espera heartbeat, registra `processed`, confirma el siguiente offset decimal con BigInt y registra `offset-committed`. Los errores se propagan. Mensajes inválidos no ejecutan acción ni commit. Fallar después de la acción y antes del commit permite repetición: no hay exactly-once, idempotencia de negocio ni DLQ.

`CONSUMER_ROLE` es obligatorio; broker/topic por defecto: `kafka:9092` y `orders.events`. `PROCESSING_DELAY_MS` es un entero opcional 0..10000, por defecto 0; recurso pedagógico, no SLA ni garantía de liveness.

## Salud y apagado

La marca `/tmp/consumer-ready` se establece tras `GROUP_JOIN`; no indica procesamiento exitoso. `DISCONNECT` y `CRASH` la eliminan. KafkaJS no reinicia por su cuenta (`restartOnFailure: false`); Compose usa `on-failure`. SIGTERM/SIGINT comparten cierre idempotente: stop antes de disconnect, deadline de 12 s y grace de 15 s en Compose. La API también cierra HTTP y conexiones con deadline de 12 s.

## Evidencia y límites

[Ejecutar el sistema completo](EJECUCION.md) · [Diez casos](casos/README.md) · [Verificación API](verificacion/api/RESUMEN.md) · [Verificación consumidores](verificacion/consumidores/RESUMEN.md).

Las pruebas locales usan dobles y no acreditan por sí solas consumo, señales ni recuperación reales. Los registros por fases describen sus respectivas corridas históricas; no son instrucciones pendientes del montaje actual. Esta reorganización documental no ejecutó runtime ni comprobó Windows nativo.
