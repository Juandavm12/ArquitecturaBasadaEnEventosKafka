# Ejecutar el laboratorio completo

Desde un clon nuevo, seguir este único recorrido para los seis servicios. Requiere Git, Docker con Compose v2 y acceso a las imágenes; Node.js >=24 solo es necesario para las pruebas locales. Los comandos de arranque son instrucciones, no una nueva evidencia de ejecución. No se ha comprobado Windows nativo.

## Proteger corridas históricas

`qa-taller-integracion-kafka` es el nombre usado en la evidencia. Si ya existe ese proyecto, **preferir un proyecto nuevo y puertos alternativos** para no mezclar pedidos, offsets ni volúmenes históricos. En Linux/WSL, configurar `export COMPOSE_PROJECT_NAME=qa-taller-integracion-kafka-reproduccion KAFKA_EXTERNAL_PORT=39092 KAFKA_UI_PORT=18080 ORDERS_API_PORT=4000`, omitir `-p qa-taller-integracion-kafka` en todos los comandos siguientes y usar API 4000/UI 18080. No detener el proyecto histórico por error. Un proyecto nuevo separa el volumen; los puertos alternativos evitan colisiones. Comprobar primero que esos puertos estén libres.

## Clonar y arrancar (shell Linux/WSL)

```sh
git clone https://github.com/Juandavm12/ArquitecturaBasadaEnEventosKafka.git
cd ArquitecturaBasadaEnEventosKafka/kafka
docker compose -p qa-taller-integracion-kafka up -d --build --wait --wait-timeout 180
docker compose -p qa-taller-integracion-kafka ps
curl --fail http://localhost:3000/health
```

La API crea `orders.events` idempotentemente al iniciar: dos particiones y RF1. No es necesario crear el topic a mano. Si ya existe con menos de dos particiones, el arranque falla; no se corrige ni se borra automáticamente. El healthcheck de los consumidores indica incorporación al grupo, no éxito de una acción de negocio. La UI no tiene healthcheck HTTP.

## Publicar un pedido nuevo y seguirlo

Cada POST genera identificadores nuevos: guardar el `orderId` devuelto y buscarlo en los logs de los tres consumidores. No reutilizar un ID histórico de los reportes como si fuera resultado de este arranque.

```sh
curl --fail -i http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerName":"Prueba local","product":"Libro","quantity":2,"unitPrice":10}'
docker compose -p qa-taller-integracion-kafka logs --timestamps orders-api inventory-consumer billing-consumer notification-consumer
for group in inventory-service-group billing-service-group notification-service-group; do
  docker compose -p qa-taller-integracion-kafka exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group "$group"
done
```

Se espera HTTP 202 después de `producer.send`, no después de facturación, reserva o notificación. Para el mismo `orderId`, contrastar `eventId`, `correlationId`, partición y offset; cada consumidor registra `received`, `processed` y `offset-committed`. El offset confirmado apunta al siguiente mensaje. Un lag cero sin correlacionar los IDs no demuestra por sí solo el recorrido de ese pedido.

Abrir <http://localhost:8080>: clúster `laboratorio`, topic `orders.events`, dos particiones y tres grupos independientes. La UI es de solo lectura. Para comprobar el topic también puede usarse:

```sh
docker compose -p qa-taller-integracion-kafka exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders.events
```

## Parar conservando datos

```sh
docker compose -p qa-taller-integracion-kafka down
```

No añadir `-v`: el volumen `kafka-data` conserva el log y los offsets entre arranques. Retención configurada: 168 horas; no se promete conservación indefinida. `fromBeginning: true` no rebobina grupos con offsets confirmados.

## Puertos y límites

Puertos publicados en loopback: API 3000 (`ORDERS_API_PORT`), UI 8080 (`KAFKA_UI_PORT`) y broker externo 29092 (`KAFKA_EXTERNAL_PORT`). Si se cambian, adaptar las URLs; los clientes internos siguen usando `kafka:9092`. KRaft de un solo nodo, sin alta disponibilidad, TLS ni autenticación: laboratorio local, no despliegue de producción.

## Recorrido equivalente PowerShell

Instrucciones no ejecutadas en Windows nativo. Para un clon nuevo, `git clone https://github.com/Juandavm12/ArquitecturaBasadaEnEventosKafka.git` y después `Set-Location .\ArquitecturaBasadaEnEventosKafka\kafka`. El bloque usa proyecto nuevo y puertos alternativos; no modifica el proyecto histórico.

```powershell
$project = 'qa-taller-integracion-kafka-reproduccion'
$env:KAFKA_EXTERNAL_PORT = '39092'
$env:KAFKA_UI_PORT = '18080'
$env:ORDERS_API_PORT = '4000'
docker compose -p $project up -d --build --wait --wait-timeout 180
docker compose -p $project ps
Invoke-RestMethod 'http://localhost:4000/health'
$pedido = @{ customerName = 'Prueba local'; product = 'Libro'; quantity = 2; unitPrice = 10 } | ConvertTo-Json
$respuesta = Invoke-RestMethod 'http://localhost:4000/orders' -Method Post -ContentType 'application/json' -Body $pedido
$respuesta
$orderId = $respuesta.orderId
docker compose -p $project logs --timestamps orders-api inventory-consumer billing-consumer notification-consumer | Select-String -SimpleMatch $orderId
$groups = @('inventory-service-group', 'billing-service-group', 'notification-service-group')
foreach ($group in $groups) {
  docker compose -p $project exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group $group
}
Start-Process 'http://localhost:18080'
# Tras revisar UI, IDs y las tres fases de cada consumidor:
docker compose -p $project down
```

No añadir `-v`. `Invoke-RestMethod` muestra el cuerpo aceptado; no se atribuye a este bloque ningún tiempo ni resultado real. Las variables de puertos permanecen en la sesión PowerShell: revisarlas antes de ejecutar comandos sobre otro proyecto.

## Pruebas locales y comprobación estática

Desde `kafka/`, con Node >=24 y dependencias instaladas (`npm ci` en una instalación nueva):

```sh
npm test
node --test test/infra.test.mjs
docker compose config --format json
```

Las 51 pruebas de contrato/API/consumidores usan dobles; las dos de infraestructura interpretan Compose sin arrancar servicios. Ninguna sustituye una prueba de integración. Las corridas y capturas históricas están en [verificación](verificacion/) y los resultados por caso en [casos](casos/README.md).
