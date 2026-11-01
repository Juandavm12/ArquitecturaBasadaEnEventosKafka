# Consumidores: montaje posterior a infraestructura y API

Ejecutar en PowerShell desde `Repo/kafka`, una vez integrada esta unidad allí.
Kafka, el topic `orders.events` y la API deben existir en el proyecto indicado.
Estos comandos son instrucciones pendientes de ejecución, no evidencia de runtime.

```powershell
$project = 'qa-taller-integracion-kafka'
$consumers = @('inventory-consumer', 'billing-consumer', 'notification-consumer')
docker compose -p $project build @consumers
docker compose -p $project up -d --no-deps --no-build --wait --wait-timeout 180 @consumers
docker compose -p $project logs --timestamps @consumers
$groups = @('inventory-service-group', 'billing-service-group', 'notification-service-group')
foreach ($group in $groups) {
  docker compose -p $project exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group $group
}
```

No crear otro pedido ni ejecutar POST. Entrada real previa: pedido
`ORD-f2035454-c380-4e9f-b682-b48ef72ca568`, partición 1, offset 0.
`fromBeginning: true` no rebobina un grupo que ya tiene offsets confirmados.
Cada rol tiene un grupo fijo distinto; inventario admite réplicas sin `container_name`
para CP10, compartiendo su grupo y repartiendo las particiones disponibles.

`CONSUMER_ROLE` es obligatorio. Broker/topic por defecto: `kafka:9092` y
`orders.events`. `PROCESSING_DELAY_MS` es opcional, entero 0..10000, por defecto 0.
Es una decisión local, al igual que el commit manual; la guía no exige delay ni
una configuración específica de autocommit. El límite es menor que la sesión
predeterminada de 30 s, pero no garantiza liveness bajo cualquier carga.

Acciones simuladas distintas: reserva de inventario, factura/cobro y notificación;
no hay DB, API externa ni email real. JSON incluye `date`, `hostname`, servicio,
grupo e instancia; los identificadores de pedido/evento/correlación están en `trace`.
Fases: `group-join`, `received`, `processed`, `offset-committed`, `fatal`,
`cleanup-error`, `shutdown-timeout`, `shutdown-complete` y `entry-error`.
El marker `/tmp/consumer-ready` acredita join, no procesamiento exitoso.

Listeners se registran antes de `run`; resolver `run` no declara readiness.
CRASH elimina marker y solicita salida 1 tras cleanup; KafkaJS no reinicia por su
cuenta (`restartOnFailure: false`), Docker usa `on-failure`. SIGTERM/SIGINT comparten
shutdown idempotente: stop antes de disconnect, deadline 12 s y grace de Compose 15 s.
El handler confirma offset sólo tras acción y heartbeat: entrega al menos una vez,
con duplicación posible si falla entre acción y commit. No hay idempotencia de negocio.
Las pruebas locales usan dobles en memoria; apagado real, señales, recuperación y
procesamiento Kafka quedan pendientes del padre tras integrar. No se afirma graceful
real ni se inventan códigos de salida de contenedores.
