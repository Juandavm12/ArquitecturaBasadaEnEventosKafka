# Núcleo de consumidores — unidad 3A

Implementación aislada, sin KafkaJS, broker, API ni negocio real. Ejecutar desde
`kafka/`: `node --test test/consumer.test.js`.

## API para el runner de unidad 3B

- `ROLE_CONFIG` / `getConfig(role)`: configuración inmutable por rol.
- `createSimulatedAction({ role, delayMs = 0, sleep })`: devuelve acción async;
  resultado `{ action, orderId }`. `sleep(ms)` es inyectable; por defecto usa Node.
- `createMessageHandler({ role, instanceId, action, logger, commitOffsets })`:
  devuelve `async eachMessage({ topic, partition, message, heartbeat })`.
  `logger(record)` puede ser síncrono o async; `commitOffsets(offsets)` debe
  enlazarse al consumidor real. La acción recibe el evento decodificado.

| Rol | Servicio | Grupo | Simulación |
| --- | --- | --- | --- |
| inventory | inventory-consumer | inventory-service-group | reserva/descuento inventario |
| billing | billing-consumer | billing-service-group | factura/cobro |
| notification | notification-consumer | notification-service-group | envío notificación |

## Contrato y límites

`received` registra el evento completo y traza; `processed` aparece tras acción
completada y heartbeat posterior; `offset-committed` solo tras commit exitoso.
Cada registro incluye acción semántica y traza con servicio, grupo, instancia,
orderId, eventId, correlationId, topic, partition y offset original.
La validación comprueba envelope OrderCreated 1.0, IDs no vacíos, source y fecha
interpretable; no repite validación comercial del productor. Offset decimal
no negativo se incrementa con BigInt y se confirma como string.

Heartbeat se espera antes/después de la acción. Los errores se propagan; mensajes
inválidos no ejecutan acción ni commit. No hay DLQ, reintentos ni mutación propia
del evento/payload. Las dependencias inyectadas deben respetar ese contrato.
Si la acción terminó y falla el heartbeat posterior o commit, puede repetirse:
no hay idempotencia ni exactly-once. Un error del logger también se propaga.

El runner integrado configura `autoCommit: false` y suscripción con
`fromBeginning: true`; este módulo no conecta ni suscribe. El delay es opt-in
pedagógico, no requisito de la guía ni garantía de liveness para acciones largas.
La guía exige tres simulaciones independientes, no APIs ni efectos reales.
Evidencia original: `../docs/verificacion/consumidores/nucleo-01/red/` y `green/`.
La integración pasó 40/40 pruebas locales; todavía no acredita consumo Kafka real.
