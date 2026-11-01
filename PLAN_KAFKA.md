# Ruta de trabajo — Kafka, anexo 2

**Objetivo:** entregar un repositorio independiente, ejecutable y reproducible con las diez ejecuciones Kafka, sus evidencias y un informe. Este plan no afirma resultados aún no obtenidos.

## Fuentes y decisiones

Fuente principal: *Taller Pruebas de Integración.pdf*, 24 páginas, en la carpeta del taller del semestre. Casos/formato: pp. 4–5; rúbrica: pp. 5–6; anexo 2 completo: pp. 14–24. El original del profesor no se modifica ni se copia automáticamente a esta entrega.

- Node.js + Express y KafkaJS, de acuerdo con la elección previa del estudiante; versiones pendientes de verificar y fijar.
- Docker Compose obligatorio, procesos separados y capturas reales.
- RabbitMQ detenido temporalmente por autorización del usuario, conservando contenedores y volumen; no repetir sus diez casos.
- Repositorio público [Juandavm12/ArquitecturaBasadaEnEventosKafka](https://github.com/Juandavm12/ArquitecturaBasadaEnEventosKafka): publicación inicial parcial 7/10. Actualización de cierre: 10/10 casos técnicamente verificados. PDF global y capturas GUI adicionales de CP-10/11/12 pendientes; vistas CLI/JSON derivadas disponibles.
- Reproducción final desde Windows PowerShell pendiente; no atribuir validación Windows nativa a ejecuciones Linux/WSL.
- Publicación inicial de los puertos solo en loopback. API 3000, Kafka externo 29092 y Kafka UI 8080; conexión interna `kafka:9092`.

## Unidades y verificación

| Unidad | Alcance | Criterio de cierre | Estado |
| --- | --- | --- | --- |
| 1 | Preparación y contrato `OrderCreated` | Entorno separado, contrato y validación offline RED/GREEN | Verificada: 21/21 tests |
| 2 | Publicación y log persistente | API + Kafka + UI, tópico de al menos dos particiones, evento visible | Verificada técnicamente y con captura GUI correlacionada; sin cerrar CP formales |
| 3 | Consumo independiente | Tres responsabilidades/grupos, acción simulada y confirmación posterior | Verificada: 6/6 servicios, tres grupos procesaron el pedido existente y confirmaron offset 1; capturas de grupos/logs recibidas y CP-05 documentado |
| 4 | Casos formales | Diez reportes de 11 campos con evidencia real y capturas | 10/10 técnicamente verificados; reportes completos. CP-10/11/12 incluyen vistas CLI/JSON derivadas, no capturas GUI |
| 5 | Entrega reproducible | Arranque/prueba/apagado PowerShell, entorno limpio aislado, informe revisado | Pendiente |

La unidad 3 se divide en **3A: núcleo y pruebas deterministas** y **3B: runner, Compose y consumo real**; cada tramo tendrá un máximo de 400 líneas de autoría, sin commits automáticos. Las esperas artificiales, si se usan para observar el procesamiento, son decisiones locales: el anexo Kafka no exige una duración. Los tres grupos leerán inicialmente el registro ya publicado, sin crear otro pedido para esta comprobación.

Cada unidad conservará sus comandos y resultados. Evitar una implementación masiva sin revisión: antes de las unidades de runtime se acordará la ampliación de alcance; no se harán commits ni publicación en remoto por inercia.

## Contrato del anexo

`POST /orders` recibe `customerName`, `product`, `quantity` y `unitPrice`. Publica en `orders.events` usando `orderId` como key y responde `status: accepted`, `orderId` y el mensaje «Pedido recibido y evento OrderCreated publicado en Kafka» después de confirmar publicación, sin esperar procesamiento de consumidores.

El evento contiene `eventId`, `eventType: OrderCreated`, `eventVersion: 1.0`, `occurredAt`, `source: orders-api`, `correlationId` y `data`. Dentro de `data`: `orderId`, `customerName`, `product`, `quantity`, `unitPrice`, `total`.

Decisiones locales de validación, no requisitos textuales del profesor: nombres no vacíos, cantidad entera positiva segura, precio numérico finito no negativo y total finito. Precio cero permitido. No se implementa lógica comercial avanzada ni precisión monetaria contable; se documentará el alcance de la simulación.

## Montaje y consumo

- Seis servicios mínimos: `orders-api`, `kafka`, `kafka-ui`, `inventory-consumer`, `billing-consumer`, `notification-consumer`.
- Grupos distintos: `inventory-service-group`, `billing-service-group`, `notification-service-group`.
- Para CP-10: escalar inventario dentro de su mismo grupo y demostrar asignaciones y procesamiento en ambas instancias. Tres grupos no demuestran reparto intragrupo.
- Sin `container_name` en consumidores escalables. Healthchecks/reintentos: `depends_on` simple no garantiza disponibilidad.
- Persistencia mediante volumen y política de retención documentada. Nunca borrar volúmenes durante recuperación.
- Confirmar offsets después de la acción simulada; registrar evento/pedido/correlación, instancia, tópico, partición y offset. No prometer exactly-once.
- `fromBeginning` no ignora offsets confirmados de un grupo existente.
- Verificar disponibilidad y compatibilidad de imágenes antes de elegirlas. El Compose del anexo es orientativo; no adoptar `latest` como versión reproducible.

## Casos formales — 10/10 técnicamente verificados y documentados

| Caso | Evidencia necesaria |
| --- | --- |
| CP-01 | **Cumple** — [reporte de 11 campos y evidencia](docs/casos/CP01/reporteCP01.md); recepción y respuesta de la petición del 31 de octubre, compartida con CP-04 |
| CP-04 | **Cumple** — [reporte de 11 campos y capturas](docs/casos/CP04/reporteCP04.md); publicación del 31 de octubre correlacionada con API, broker y Kafka UI |
| CP-05 | **Cumple** — [reporte de 11 campos y capturas](docs/casos/CP05/reporteCP05.md); mismo evento en los tres consumidores |
| CP-06 | **Cumple** — [reporte de trazabilidad y evidencia](docs/casos/CP06/reporteCP06.md); IDs explícitos en API, payload del log persistente del broker y tres fases de cada consumidor |
| CP-07 | **Cumple** — [reporte Responsive y evidencia](docs/casos/CP07/reporteCP07.md); HTTP 202/239 ms frente a inventario demorado experimentalmente 3000 ms |
| CP-08 | **Cumple** — [reporte Message-driven y evidencia](docs/casos/CP08/reporteCP08.md); flujo auditado por Kafka, CP-07 compartido y hashes posteriores |
| CP-09 | **Cumple** — [reporte Resilient y evidencia](docs/casos/CP09/reporteCP09.md); continuidad durante parada controlada del consumidor |
| CP-10 | **Cumple** — [Elastic](docs/casos/CP10/reporteCP10.md): dos miembros del mismo grupo, cuatro pedidos repartidos 2+2, lag 0 y réplica retirada |
| CP-11 | **Cumple** — [Desacoplamiento temporal](docs/casos/CP11/reporteCP11.md): HTTP 202 con inventario apagado y consumo posterior del mismo evento |
| CP-12 | **Cumple** — [Recuperación](docs/casos/CP12/reporteCP12.md): evento pendiente recuperado tras stop/start del mismo inventario; lag 1→0. Comparte un pedido con CP-11 |

Cada reporte tendrá ID, arquitectura, objetivo, precondiciones, datos de entrada, pasos, resultado esperado, resultado obtenido, evidencia/capturas, estado y análisis técnico. No completar resultados ni marcar Cumple antes de ejecutar y revisar las evidencias.

## Alertas de lectura de la guía

Los diagramas de pp. 3/14 muestran `PedidoCreado`, `pedidos-creados` y workers genéricos. Se sigue el contrato operativo explícito del anexo: `OrderCreated`, `orders.events` y tres responsabilidades. La nota p. 20 prohíbe `container_name` para escalar, aunque el ejemplo de p. 22 lo incluye: se omitirá en consumidores.

## Verificación de la unidad 1

`npm test`: RED exitcode 1 antes de crear el módulo; GREEN exitcode 0, 21/21 pruebas. Resultados originales en [docs/verificacion/contrato](docs/verificacion/contrato). Node host v26.8.1; ejecución Windows nativa no verificada. LSP sin errores de código reportados; seis avisos informativos del corrector inglés sobre palabras españolas, no defectos funcionales. No fue necesario refactorizar el módulo tras GREEN.

Se comprobó que RabbitMQ conserva los cuatro IDs de contenedor y el montaje del broker tras `docker compose stop` (comando exitcode 0). Broker final exitcode 0; API y workers final exitcode 137, registrados sin ocultarlos. No se afirma apagado graceful de las aplicaciones. Registro de preparación fuera del repo entregable, en `evidencias/kafka/preparacion-entorno/` del taller. No hubo publicaciones nuevas ni eliminación de datos.

## Infraestructura parcial verificada

[Resumen y fuentes originales](docs/verificacion/infraestructura/RESUMEN.md). La fase inicial incorporó Kafka + Kafka UI (2/6); arquitectura final de seis contenedores confirmada por el usuario. Imágenes versionadas Apache Kafka 3.9.1 y Kafka UI 0.7.2 descargadas, digests conservados. Broker healthy, `orders.events` creado con 2 particiones/RF1; UI HTTP200 y clúster `laboratorio` online/readOnly. Bootstrap externo ApiVersions desde host correcto. Tests integrados: 21/21 contrato + 2/2 configuración, sin cerrar CP.

**Actualización API:** [verificación real](docs/verificacion/api/RESUMEN.md), **3/6 servicios**, 29/29 pruebas con Node 26 y Node 24 más 2/2 Compose. Un POST obtuvo 202/0.381403 s: `ORD-f2035454-c380-4e9f-b682-b48ef72ca568`, partición 1/offset 0, relacionado con el log nativo del broker; offsets 0/0 → 0/1. No hay grupos consumidores todavía ni CP cerrados. Captura GUI del evento recibida y archivada con SHA-256, JSON completo y mismos IDs/partición/offset; no se repitió el POST. La lectura del visor UI no demuestra procesamiento de consumidores funcionales.

Capturas GUI iniciales recibidas y verificadas: clúster online y `orders.events` con 2 particiones/RF1/0 mensajes; conservadas sin modificar en `docs/verificacion/infraestructura/capturas/`. Recuperación con eventos y capturas de los casos pendientes. Runtime verificado en Linux/WSL, no Windows nativo. El despliegue mantiene RabbitMQ detenido sin cambios.

**Actualización consumidores:** [seis servicios y consumo independiente verificados](docs/verificacion/consumidores/RESUMEN.md), 51/51 pruebas en Node 26 y Node 24, 2/2 Compose. Sin otro POST; los tres grupos confirmaron offset 1 de partición 1, lag 0. **Cierre documental CP-05:** [reporte de consumo independiente](docs/casos/CP05/reporteCP05.md), **Cumple**. Capturas de grupos/logs registradas el 31 de octubre de 2026 y correlacionadas con el procesamiento del 27 de octubre; no hubo otra publicación. La UI muestra lag N/A; el lag 0 citado procede de las consultas nativas históricas. **Cierre documental CP-04:** [reporte de publicación](docs/casos/CP04/reporteCP04.md), **Cumple**, con la nueva corrida del 31 de octubre: un POST desde Postman CLI, 202/370 ms, cinco verificaciones aprobadas y evento en partición 0/offset 0 confirmado por API, broker y Kafka UI. CP-05 mantiene su corrida histórica independiente. **Cierre documental CP-01:** [reporte de creación vía API](docs/casos/CP01/reporteCP01.md), **Cumple**, con la misma petición Postman CLI del 31 de octubre compartida con CP-04, sin otro POST. **Cierre documental CP-06:** [trazabilidad histórica](docs/casos/CP06/reporteCP06.md), **Cumple**: pedido y correlación coinciden en API, log persistente del broker y consumidores del 27 de octubre, sin repetir el flujo. El hito publicado inicial quedó en **7/10**. La nueva corrida mínima documentada más abajo completa localmente CP-10/11/12, sin reescribir esa evidencia histórica. [CP-09 — Resilient: Cumple](docs/casos/CP09/reporteCP09.md), parada controlada de inventario, HTTP 202 y continuidad de los otros dos consumidores; restauración verificada. Ese cierre histórico no acreditaba CP-10/11/12; la nueva prueba independiente sí los documenta. **Cierre documental CP-08:** [Message-driven](docs/casos/CP08/reporteCP08.md), flujo PoC auditado mediante Kafka; reutilización CP-07 y hashes posteriores, sin otra publicación. **Cierre documental CP-07:** [Responsive](docs/casos/CP07/reporteCP07.md), respuesta observada 2991 ms antes de processed; condición experimental, no SLA. En aquella corrida no se probó recuperación de pendientes ni reparto intragrupo.

**Cierre mínimo posterior (1 de noviembre de 2026):** [evidencia, comandos y límites](docs/verificacion/cierre-minimo-20261101/RESUMEN.md). Una publicación con inventario apagado acredita pendiente p1 current/end=3/4 lag=1; start del mismo contenedor recupera y confirma ese evento, current/end=4/4 lag=0 (CP-11 y CP-12). Una réplica local temporal comparte inventory-service-group; cuatro pedidos se reparten 2+2 entre ambas instancias (CP-10). Réplica detenida y retirada; inventario original asume ambas particiones con lag 0 (p0=3/3, p1=6/6). Cinco POST totales, sin cambios de código ni infraestructura persistente. Doce comprobaciones offline PASS y suite existente con exit 0. Estado técnico local **10/10**; PDF global, reproducción Windows nativa y capturas GUI adicionales siguen fuera del cierre. Vistas SVG explícitamente derivadas de CLI/JSON, no capturas de interfaz.

## Evidencias y entrega

Conservar comandos, inicio/fin, stdout/stderr, exitcodes, IDs, logs originales y capturas legibles por escenario/corrida. Diferenciar capturas GUI de vistas derivadas de logs. No modificar originales para ocultar fallos.

Primero completar Kafka; después comparar empíricamente con RabbitMQ y preparar el informe global del taller. RabbitMQ 10/10 no implica Kafka completo ni taller 20/20.
