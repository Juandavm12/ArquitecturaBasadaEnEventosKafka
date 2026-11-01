# Infraestructura Kafka — broker y UI verificados

**Resultado:** Kafka arrancó healthy y Kafka UI conecta al clúster `laboratorio` online. `orders.events` existe con **dos particiones y factor de replicación 1**. Esta verificación inicial correspondió a **2 de los 6 servicios finales**. La [actualización API](../api/RESUMEN.md) añade el tercer servicio y el primer evento; estas capturas iniciales del tópico vacío son históricas. Kafka sigue **0/10 casos formales**.

## Evidencia original

| Verificación | Resultado y archivos |
| --- | --- |
| Descarga e identificación de imágenes | Ambos pulls e inspect exitcode 0: [comandos](imagenes/commands.json), [Kafka](imagenes/kafka-inspect.json), [UI](imagenes/kafka-ui-inspect.json) |
| TDD de configuración en staging | RED exitcode 1 antes de Compose, GREEN exitcode 0, 2/2 tests: [RED](estatica-staging/red.log), [GREEN](estatica-staging/green.log) |
| Suite integrada | Contrato 21/21 e infraestructura 2/2, exitcode 0: [comandos](estatica-integrada/commands.json) |
| Arranque Kafka y tópico | Cinco comandos exitcode 0: [registro](runtime-broker/commands.json), [describe](runtime-broker/describir-topico-stdout.txt), [estado](runtime-broker/estado-stdout.txt), [logs](runtime-broker/logs-stdout.txt) |
| Bootstrap externo desde host | Solicitud Kafka ApiVersions v0 a `127.0.0.1:29092`, correlationId 99, errorCode 0, 61 APIs: [resultado](runtime-broker/external-bootstrap.json). No prueba publicación ni consumo |
| Arranque UI y HTTP | Arranque/estado/logs exitcode 0, índice HTTP 200: [registro](runtime-ui/commands.json), [reintentos de readiness](runtime-ui/http-attempts.json) |
| Conexión interna UI → broker | [Clúster](runtime-ui/clusters.json): online, readOnly=true, un broker y dos particiones online |
| Tópico desde UI | [Respuesta HTTP 200](runtime-ui/topics-body.txt): `orders.events`, dos particiones, RF=1, ninguna infrarreplicada |
| Captura GUI del clúster | [Dashboard aportado por el usuario](capturas/cluster-online.png): `laboratorio` readonly, 1 clúster online, 0 offline, 1 broker, 2 particiones, 1 tópico y producción/consumo 0 Bytes |
| Captura GUI del tópico | [Topics aportado por el usuario](capturas/orders-events-dos-particiones.png): `orders.events`, 2 particiones, 0 réplicas fuera de sincronización, RF=1, 0 mensajes y tamaño 0 Bytes |

Ambas capturas se revisaron visualmente y se conservaron sin modificaciones, con igualdad SHA-256 frente a los originales: [integridad](capturas/integridad.json). La versión `3.9-IV0` mostrada en el dashboard es la reportada por la UI; la etiqueta de imagen ensayada es `apache/kafka:3.9.1`. No se infiere fecha de captura ni se presentan estas imágenes como evidencia de publicación o consumo.

Fechas y horas exactas están en los registros originales. Verificación realizada en Docker Desktop/Linux desde WSL; no acredita ejecución nativa Windows/Hyper-V.

## Reproducción

Seguir [INFRAESTRUCTURA.md](../../../kafka/INFRAESTRUCTURA.md), desde `kafka/`, con el proyecto **`qa-taller-integracion-kafka`**. Se documentaron listeners internos `kafka:9092`, externos `localhost:29092` y UI `localhost:8080`, todos los puertos publicados en loopback. No usar un proyecto Compose diferente al consultar este entorno.

El volumen nombrado conserva `/var/lib/kafka/data`. No se borró ningún volumen. Todavía no se publicó ningún evento ni se hizo un ensayo de reinicio con datos: no confundir persistencia configurada con recuperación demostrada.

## Pendiente

- API `orders-api` y los tres consumidores, con grupos independientes y confirmación posterior al procesamiento.
- Publicación observable, pruebas formales con evidencia/capturas y recuperación de pendientes.
- Validar reproducción completa y elaborar informe.

Los logs de Kafka UI indican Kafka 3.5.0 para su cliente Java embebido; eso no cambia la imagen del broker ensayado, Apache Kafka 3.9.1. El broker de un nodo/RF=1 no demuestra alta disponibilidad.

La revisión estática LSP no reportó errores, pero una de sus dos comprobaciones no concluyó dentro de 1 s; no se presenta como cobertura completa. Las pruebas `docker compose config` y la verificación runtime anterior sí terminaron con los resultados registrados.
