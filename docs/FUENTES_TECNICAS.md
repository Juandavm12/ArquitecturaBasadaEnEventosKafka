# Fuentes técnicas del montaje Kafka

La guía del profesor sigue siendo la fuente de requisitos. Estas fuentes oficiales respaldan la adaptación técnica del ejemplo; no sustituyen la guía ni acreditan ejecución.

| Fuente | Uso |
| --- | --- |
| [Apache Kafka 3.9.1: guía de la imagen Docker](https://github.com/apache/kafka/blob/3.9.1/docker/examples/README.md) | Variables `KAFKA_*`, `CLUSTER_ID`, listeners y factores de replicación de un nodo |
| [Apache Kafka 3.9.1: ejemplo KRaft de un nodo](https://github.com/apache/kafka/blob/3.9.1/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml) | Separación de listener interno/externo y controlador combinado |
| [Kafka UI v0.7.2: ejemplo Compose](https://github.com/provectus/kafka-ui/blob/v0.7.2/documentation/compose/kafka-ui.yaml) | Variables `KAFKA_CLUSTERS_0_NAME` y `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` |
| [KafkaJS: producción](https://kafka.js.org/docs/producing) | `producer.send`, confirmaciones `acks: -1`, key, headers y particionador |
| [KafkaJS: configuración](https://kafka.js.org/docs/configuration) | Brokers iniciales, descubrimiento de metadata, timeouts y reintentos acotados |

## Adaptación respecto al anexo 2

Se selecciona la imagen oficial versionada `apache/kafka:3.9.1` en lugar de `bitnamilegacy/kafka:3.7` del ejemplo. No se usan variables `KAFKA_CFG_*` de Bitnami con la imagen Apache. Kafka UI usa `provectuslabs/kafka-ui:v0.7.2`, no `latest`.

Son versiones seleccionadas para este laboratorio, no una afirmación de ser las más recientes ni una certificación de seguridad. La disponibilidad efectiva, plataforma y digest se conservan en `verificacion/infraestructura/imagenes/`. Fijar una etiqueta mejora la reproducción; los digests observados permiten identificar exactamente las imágenes ensayadas.

## Semántica de publicación de la API

La API esperará la confirmación de publicación de Kafka, no el procesamiento de los consumidores. `acks: -1` requiere confirmación de las réplicas en sincronía; en este laboratorio de un broker/RF=1 sigue existiendo una sola copia replicada.

HTTP 202 indicará publicación confirmada, no inventario reservado, factura generada ni notificación enviada. Un error o timeout puede dejar un resultado ambiguo: el broker podría haber almacenado el evento aunque el cliente no recibiera confirmación. No se promete idempotencia de peticiones HTTP ni exactly-once; repetir una petición puede crear otro pedido.

## Límites

Un solo broker/controlador KRaft y factor de replicación 1 permiten estudiar persistencia y offsets, pero no alta disponibilidad. Puertos de laboratorio en loopback, sin TLS ni autenticación; no desplegar esta configuración expuesta en Internet.
