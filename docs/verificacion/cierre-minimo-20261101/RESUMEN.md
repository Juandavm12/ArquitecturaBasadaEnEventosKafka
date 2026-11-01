# Cierre mínimo real — CP-10, CP-11 y CP-12

**Los tres casos cumplen técnicamente en la muestra probada.** Ejecución Linux/WSL del 1 de noviembre de 2026, UTC. Cinco POST en total, un stop/start del mismo inventario y una réplica temporal posteriormente retirada. Sin instalaciones, rebuilds, cambios de código, broker, volúmenes, RabbitMQ ni configuración persistente.

## Resultados

| Caso | Evidencia decisiva | Reporte |
| --- | --- | --- |
| CP-10 | Dos miembros del mismo grupo; cuatro pedidos repartidos 2+2; fases procesado/confirmado y lag 0 | [Elastic](../../casos/CP10/reporteCP10.md) |
| CP-11 | HTTP 202 con inventario apagado; lag 1 antes de start; mismo evento procesado después | [Desacoplamiento](../../casos/CP11/reporteCP11.md) |
| CP-12 | Evento pendiente partición 1/offset 3 recuperado por el mismo contenedor; current 3→4 y lag 1→0 | [Recuperación](../../casos/CP12/reporteCP12.md) |

[Validación offline: 12 comprobaciones PASS](validacion.json) · [IDs y trazas correlacionados](resumen-trazas.json) · [SHA-256 de evidencias](manifest.json). `npm-test.*` conserva la suite existente y su exitcode; no se añade otro arnés.

## Secuencia y comandos ejecutados

Registro documental de esta sesión, no script para reejecutar automáticamente. Los comandos mutadores requieren autorización nueva para otra corrida. Las salidas originales separan stdout/stderr; los bloques shell usan `set -eu`, por lo que un fallo habría detenido la secuencia.

Nombres usados: inventario `qa-taller-integracion-kafka-inventory-consumer-1`; broker `qa-taller-integracion-kafka-kafka-1`; réplica `kafka-cp10-minima-20261101`.

| Orden | Operación ejecutada | Resultado observado / archivo |
| --- | --- | --- |
| 1 | `docker ps -a`; logs recientes; consulta `--describe --group inventory-service-group --state` | Seis originales running; inventario healthy pero grupo Empty/0. No se confunde healthcheck con membresía |
| 2 | `curl --fail --silent --show-error http://127.0.0.1:3000/health` | Exit 0, `status: ok` |
| 3 | `docker inspect --format ...` de inventario; `docker stop --time 15` del inventario | Exit 0; `inventory-antes.txt`, `stop.*`, `inventory-apagado.txt` |
| 4 | Un `curl --fail-with-body --silent --show-error -D ... -o ... -w ... -H 'Content-Type: application/json' --data-binary @pedido-pendiente.request.json http://127.0.0.1:3000/orders` | Exit 0, HTTP 202; `pedido-pendiente.*` |
| 5 | `docker exec` broker `/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group inventory-service-group` | Job 1 exit 0; `pendiente-offsets.*`, p1 current/end=3/4, lag 1, sin miembro |
| 6 | `docker start` del mismo inventario; inspect | Exit 0; `start.*`, `inventory-reiniciado.txt` |
| 7 | Logs de inventario/API y consulta de offsets | Job 3 exit 0; `recuperacion-*`, `recuperado-offsets.*`; lag 0 |
| 8 | `docker run --detach --pull=never --name kafka-cp10-minima-20261101 --network qa-taller-integracion-kafka_default -e CONSUMER_ROLE=inventory -e KAFKA_BROKER=kafka:9092 -e KAFKA_TOPIC=orders.events -e PROCESSING_DELAY_MS=0 sha256:8678df89f52018880ad6fa71a5ce8932743d3998c3a68243f1dbf0e4c10d8ab6 node src/consumer-server.js` | Exit 0; `replica.id.txt`, `replica.run.stderr.txt` |
| 9 | Consulta `--describe --group inventory-service-group --members --verbose`; inspect de imagen/entorno; logs iniciales réplica | Job 4 exit 0; `cp10-miembros.*`, `cp10-imagen-env.txt`, `cp10-replica-inicial.*` |
| 10 | Cuatro curl POST, mismos flags del paso 4; logs de ambas instancias/API | Bloque exit 0; `cp10-pedido-1..4.*`, `cp10-original.*`, `cp10-replica.*`, `cp10-api.*` |
| 11 | Consulta de offsets; `docker stop --time 15` réplica; logs e inspect; `docker rm` únicamente réplica; consulta final e inspect originales | Job 5 exit 0; `cp10-offsets.*`, `replica.stop.*`, `replica-final.*`, `replica.rm.*`, `final-*`, `inventory-final.txt` |
| 12 | Comprobación offline mediante Node: correlación de IDs, fases, asignaciones, imágenes, offsets y limpieza | Exit 0; 12 comprobaciones PASS en `validacion.json` |
| 13 | `npm test` en `kafka/` | Job 6 exit 0; salida y código en `npm-test.*` |

Un monitor de logs de inventario de 60 segundos mostró recuperación y rebalance; terminó por su límite de observación. No se contabiliza como prueba funcional aprobada ni como timeout de los comandos Kafka. No se reintentó el monitor. Las pruebas se sustentan en logs y consultas nativas terminadas con exit 0.

## Evidencia y límites

- `inicio.txt`, `publicado-apagado.txt`, `start.inicio.txt`, fechas de consultas y `fin.txt` conservan marcas de la secuencia del host; no son snapshots atómicos del clúster.
- CP-11 y CP-12 comparten **el mismo pedido** y distinguen sus objetivos. No se suman como dos publicaciones ni se atribuyen a la corrida histórica CP-09.
- Las imágenes SVG de los reportes son **vistas documentales derivadas de CLI/JSON**, no capturas GUI. Si el docente exige pantallazos de interfaz, quedan por adjuntar; no se afirma haber abierto Postman o Kafka UI en esta corrida.
- Al iniciar, inventario estaba healthy con grupo vacío. El stop/start autorizado permitió observar membresía y procesamiento; no se atribuye una causa raíz no investigada. No se certifica salud de facturación/notificaciones ni se intervienen esos servicios.
- Inventario final conserva su ID e imagen originales y asume ambas particiones con lag 0. No queda la réplica. Seis contenedores originales permanecen running.
- No se prueba crash abrupto, reinicio de broker, deduplicación, rendimiento ni exactly-once. El PDF global y la reproducción Windows nativa no forman parte de este cierre técnico.
- Documentación pasiva y pruebas reales sobre código existente: no aplica inventar un ciclo RED/GREEN ni modificar la aplicación para justificarlo. Evidencias históricas y saneamiento previo permanecen intactos.
