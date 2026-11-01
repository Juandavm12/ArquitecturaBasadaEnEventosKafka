# CP-05 — Consumo de evento Kafka

## ID del caso

CP-05 — Consumo de evento (Kafka).
Reporte documental de evidencia histórica; no registra una ejecución nueva.

## Arquitectura

Kafka: API productora → `orders.events` → tres consumidores independientes.
Cada responsabilidad usa un grupo distinto y su propio contenedor.
Kafka UI es observación, no un consumidor funcional de negocio.

## Objetivo

Validar que los consumidores reciben el mismo `OrderCreated`.
Se contrasta recepción, acción simulada y confirmación posterior por grupo.
Criterio tomado de la guía, p. 4; anexo 2, pp. 20–24.

## Precondiciones

- Evento histórico existente en `orders.events`, partición 1, offset 0.
- Seis servicios registrados activos en la verificación del 27 de octubre de 2026.
- Grupos independientes: inventario, facturación y notificaciones.
- Fuente de contexto: [resumen histórico](../../verificacion/consumidores/RESUMEN.md).
- La revisión documental no consulta ni altera el estado actual del entorno.

## Datos de entrada

- `orderId`: `ORD-f2035454-c380-4e9f-b682-b48ef72ca568`.
- `eventId`: `657b734d-55ed-4d10-ba08-5277b9eba6b2`.
- `correlationId`: `cdb824fe-f331-4a6d-8c84-2dbb89ba6566`.
- Tipo/versión/origen: `OrderCreated` / `1.0` / `orders-api`.
- `occurredAt`: `2026-10-27T12:19:38.136Z`, según evento recibido.
- Datos del evento: Pedro, Libro de arquitectura, cantidad 1, precio 85000, total 85000.
- Entrada del consumidor: registro persistido; no se envió otro POST para documentar este caso.

## Pasos ejecutados

1. En la corrida histórica se iniciaron los tres consumidores contra el registro existente.
2. Se recogieron logs separados con `received`, `processed` y `offset-committed`.
3. Se consultó nativamente el estado de offsets de cada grupo.
4. Se conservaron [comandos, tiempos y exitcodes originales](../../verificacion/consumidores/runtime-01/commands.json).
5. El 31 de octubre se registraron capturas de grupos y logs históricos del 27.
6. En esta unidad solo se revisaron archivos y se realizó comprobación estructural offline.

Los pasos 1–4 describen evidencia previa, no comandos repetidos ahora.
El registro del 31 no convierte el procesamiento del 27 en una ejecución nueva.

## Resultado esperado

Los tres consumidores reciben el mismo evento, conservando sus identificadores.
Cada uno registra su reacción simulada y confirma después del procesamiento.
Los grupos tienen progreso independiente sobre el mismo tópico/partición.
La guía no exige un tiempo mínimo ni operaciones comerciales reales para CP-05.

## Resultado obtenido

Los tres logs contienen la misma terna pedido/evento/correlación indicada arriba.
Tiempos UTC de los campos `date`, todos del **27 de octubre de 2026**:

| Servicio / grupo | Instancia | Acción simulada | received → processed → offset-committed (UTC) | Partición / registro / siguiente confirmado |
| --- | --- | --- | --- | --- |
| inventory-consumer / inventory-service-group | b8d3a808fc2c | simulate-stock-reservation | 13:34:57.080 → 13:34:57.080 → 13:34:57.091 | 1 / 0 / 1 |
| billing-consumer / billing-service-group | 5caff4b88c36 | simulate-invoice-charge | 13:34:56.687 → 13:34:56.688 → 13:34:56.820 | 1 / 0 / 1 |
| notification-consumer / notification-service-group | 3ce4bcc7867e | simulate-notification-send | 13:34:56.664 → 13:34:56.681 → 13:34:56.823 | 1 / 0 / 1 |

IDs comunes a las tres filas: `orderId`, `eventId` y `correlationId` de Datos de entrada.
El offset 0 de `trace` identifica el registro; 1 es la próxima posición confirmada.
Las consultas nativas muestran CURRENT-OFFSET 1, LOG-END-OFFSET 1 y LAG 0 en partición 1.
La partición 0 vacía muestra CURRENT-OFFSET/LAG `-`, LOG-END-OFFSET 0; no se interpreta como error.
La UI muestra tres grupos STABLE, un miembro y un tópico por grupo; su LAG es **N/A**, no cero.
El lag cero procede exclusivamente de las consultas nativas históricas del 27.

## Evidencia (capturas)

Capturas registradas el **31 de octubre de 2026**, originales sin editar.
Los PNG son evidencia visual; los JSON asociados conservan SHA-256, tamaño y observaciones.

- [Grupos Kafka UI](../../verificacion/reanudacion/20261031T170229Z-api/capturas/02-kafka-ui-tres-grupos.png) · [metadatos](../../verificacion/reanudacion/20261031T170229Z-api/capturas/02-kafka-ui-tres-grupos.json).
- [Inventario](../../verificacion/reanudacion/20261031T170229Z-api/capturas/03-inventory-consumer-procesamiento.png) · [metadatos](../../verificacion/reanudacion/20261031T170229Z-api/capturas/03-inventory-consumer-procesamiento.json).
- [Facturación](../../verificacion/reanudacion/20261031T170229Z-api/capturas/04-billing-consumer-procesamiento.png) · [metadatos](../../verificacion/reanudacion/20261031T170229Z-api/capturas/04-billing-consumer-procesamiento.json).
- [Notificación](../../verificacion/reanudacion/20261031T170229Z-api/capturas/05-notification-consumer-procesamiento.png) · [metadatos](../../verificacion/reanudacion/20261031T170229Z-api/capturas/05-notification-consumer-procesamiento.json).

Fuentes directas para contrastar las capturas:

- [Log inventario](../../verificacion/consumidores/runtime-01/inventory-consumer-logs-stdout.txt) · [describe inventario](../../verificacion/consumidores/runtime-01/inventory-service-group-describe-stdout.txt).
- [Log facturación](../../verificacion/consumidores/runtime-01/billing-consumer-logs-stdout.txt) · [describe facturación](../../verificacion/consumidores/runtime-01/billing-service-group-describe-stdout.txt).
- [Log notificación](../../verificacion/consumidores/runtime-01/notification-consumer-logs-stdout.txt) · [describe notificación](../../verificacion/consumidores/runtime-01/notification-service-group-describe-stdout.txt).
- [Resultado correlacionado histórico](../../verificacion/consumidores/runtime-01/result.json).
- [Código de recepción, simulación y commit](../../../kafka/src/consumer.js).

## Estado

**Cumple** para CP-05: la evidencia combinada acredita recepción del mismo evento en los tres servicios.
Es una conclusión técnica propia, no una calificación ni aprobación del profesor.
Se cierra únicamente este reporte: Kafka **1/10**, con nueve casos pendientes.
El resultado histórico conserva `formalCasesClosed: 0`: no se reescribe retroactivamente.

## Análisis técnico

Los grupos distintos permiten fan-out: cada responsabilidad lee el mismo registro.
La membresía STABLE no basta por sí sola: los IDs y fases de los logs acreditan recepción y procesamiento.
El código espera la acción simulada antes de confirmar `offset + 1`; el describe respalda el avance por grupo.
No se borra el evento al consumirlo: se avanza una posición independiente por grupo.
Las acciones no reservan inventario en DB, no cobran ni generan facturas reales, ni envían email.
Los logs originales contienen `TimeoutNegativeWarning` y errores iniciales de coordinador no disponible.
Posteriormente hay join, procesamiento y commit; no se afirma ausencia de errores ni se diagnostica su causa raíz.
Esta evidencia no demuestra idempotencia, exactly-once ni ausencia de duplicados en otras corridas.
Tres grupos no prueban reparto intragrupo (CP-10); tampoco recuperación de pendientes (CP-12).
No se cierra CP-06 ni otro caso por reutilizar estos identificadores.
No se ejecutaron runtime, POST, reinicios ni pruebas de otros casos durante esta documentación.
