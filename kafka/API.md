# API de pedidos — fase 3/6

Incluye broker, UI y orders-api; faltan los consumidores de la arquitectura final.
No se declara cumplimiento CP ni integración real sin reporte de ejecución.

## Ruta rápida (PowerShell)

Desde esta carpeta, cuando esté autorizado ejecutar Docker:

```powershell
docker compose -p qa-taller-integracion-kafka up -d --build --wait --wait-timeout 180
Invoke-RestMethod http://localhost:3000/health
$pedido = @{ customerName = 'Ana'; product = 'Libro'; quantity = 2; unitPrice = 10 } | ConvertTo-Json
Invoke-RestMethod http://localhost:3000/orders -Method Post -ContentType 'application/json' -Body $pedido
docker compose -p qa-taller-integracion-kafka logs orders-api
```

`ORDERS_API_PORT` permite cambiar el puerto publicado; ajustar las URLs.
La imagen base Node 24 está fijada por digest en el Dockerfile; su identificación
se conserva en `../docs/verificacion/api/imagen-node-reanudacion-01/`.

## Contrato y límites

- `POST /orders`: JSON hasta 32kb; valida nombres no vacíos, cantidad entera
  positiva y precio finito no negativo. Responde 400 por entrada inválida, 413
  por tamaño, 503 por fallo Kafka.
- El 202 contiene `status: accepted`, `orderId` y el mensaje
  `Pedido recibido y evento OrderCreated publicado en Kafka`.
  Solo se envía tras confirmar `producer.send`; no espera consumidores ni
  confirma acciones de negocio.
- Publica `OrderCreated` en `orders.events`, key `orderId`, headers `eventId`
  y `correlationId`, acks -1 y timeout 10000ms. No promete idempotencia.
- Un HTTP 503 puede ser un resultado ambiguo: Kafka pudo recibir el evento.
  Repetir la solicitud HTTP puede crear otro pedido.
- `GET /health` consulta metadata, sin publicar; exige al menos dos particiones.
  Startup crea el topic idempotentemente (dos particiones, RF1), conecta el
  productor antes de escuchar y falla si un topic existente tiene menos de dos.
- Logs JSON: fecha (`date`), servicio, hostname, IDs, topic y metadata
  `partition`/`baseOffset` en publicación; errores internos no salen por HTTP.
- SIGINT/SIGTERM cierran HTTP y conexiones Kafka con deadline de 12s;
  Compose concede 15s. No hay reintentos indefinidos ni autocreación del productor.

## Verificación local sin Kafka

`npm test` ejecuta contrato original y API con dobles locales.
`node --test test/infra.test.mjs` comprueba Compose por separado (sin daemon).
Evidencia de comandos, tiempos, salida y exit code:
`../docs/verificacion/api/staging-reanudacion-01/`.
Las pruebas locales no certifican el startup, Docker ni entrega real a Kafka.
