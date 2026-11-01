# Arquitectura basada en eventos con Kafka

Prueba de concepto del taller de Pruebas de Integración de Aseguramiento de la Calidad. **Kafka: 10/10 casos técnicamente verificados y documentados (CP-01 y CP-04 a CP-12).** CP-10/11/12 se cerraron con cinco pedidos en una prueba mínima real. Las vistas de CP-10/11/12 son derivadas de CLI/JSON, no capturas GUI. No se realizó reproducción Windows nativa.

## Estado

- [Publicación saneada y manifiestos históricos](PUBLICACION.md): tres redacciones identificadas; originales privados conservados.

- Requisitos de la guía y anexo 2: creación, publicación, consumo, trazabilidad y propiedades reactivas; cada reporte enlazado detalla objetivo, resultados y análisis técnico.
- RabbitMQ permanece separado; no se copia su código ni sus resultados como evidencia Kafka.
- Contrato `OrderCreated` implementado y validado offline.
- Infraestructura broker + UI verificada: broker healthy, UI online y `orders.events` con dos particiones. [Comandos PowerShell](kafka/INFRAESTRUCTURA.md) · [resultados reales](docs/verificacion/infraestructura/RESUMEN.md). Esta verificación inicial se complementa con las pruebas de API y consumidores indicadas a continuación.
- API verificada: HTTP 202, un `OrderCreated` en partición 1/offset 0, mismo pedido en API y log nativo del broker. [Evidencia y límites](docs/verificacion/api/RESUMEN.md). Actualmente **6/6 servicios**, con consumo real en tres grupos: [resultados](docs/verificacion/consumidores/RESUMEN.md).
- Captura GUI del primer evento recibida y correlacionada, original y SHA-256 en la evidencia anterior.
- [CP-05 — Consumo de evento Kafka: Cumple](docs/casos/CP05/reporteCP05.md). Tres consumidores recibieron y procesaron el mismo evento; logs del 27 de octubre y capturas registradas el 31 de octubre de 2026. Capturas de grupos y de los tres consumidores conservadas con SHA-256.
- [CP-04 — Publicación de evento Kafka: Cumple](docs/casos/CP04/reporteCP04.md). Corrida del 31 de octubre: Postman CLI 202/370 ms, cinco verificaciones aprobadas y el mismo evento en API, almacenamiento nativo y Kafka UI; partición 0/offset 0. Fuentes y capturas conservadas con SHA-256.
- [CP-01 — Creación de pedido vía API: Cumple](docs/casos/CP01/reporteCP01.md). Recepción y respuesta verificadas con la misma petición del 31 de octubre utilizada en CP-04; sin otro POST.
- [CP-06 — Trazabilidad del flujo: Cumple](docs/casos/CP06/reporteCP06.md). Mismos identificadores en API, log persistente del broker y las tres fases de cada consumidor; evidencia histórica del 27 de octubre.
- [CP-07 — Responsive: Cumple](docs/casos/CP07/reporteCP07.md). HTTP 202/239 ms; respuesta observada 2991 ms antes del fin de inventario con demora experimental de 3000 ms, no SLA.
- [CP-08 — Message-driven: Cumple](docs/casos/CP08/reporteCP08.md). Flujo auditado mediado por Kafka, evidencia CP-07 compartida y auditoría de hashes posterior; sin otra publicación.
- [CP-09 — Resilient: Cumple](docs/casos/CP09/reporteCP09.md), parada controlada de inventario, HTTP 202 y continuidad de los otros dos consumidores; restauración histórica verificada.
- [CP-10 — Elastic: Cumple](docs/casos/CP10/reporteCP10.md). Dos instancias del mismo grupo, cuatro pedidos repartidos 2+2 y confirmados; réplica temporal retirada.
- [CP-11 — Desacoplamiento temporal: Cumple](docs/casos/CP11/reporteCP11.md). Publicación con inventario apagado y procesamiento posterior del mismo evento.
- [CP-12 — Recuperación: Cumple](docs/casos/CP12/reporteCP12.md). Mismo contenedor recupera el evento pendiente; lag 1→0, IDs y offset correlacionados. Comparte un único pedido con CP-11.
- [Cierre mínimo: comandos, evidencia original y límites](docs/verificacion/cierre-minimo-20261101/RESUMEN.md). Sin cambios de aplicación ni infraestructura persistente.
- Alcance de la evidencia: vistas derivadas de CLI/JSON identificadas como tales; no se afirma validación Windows nativa, alta disponibilidad ni exactly-once.
- Repositorio de entrega: [Juandavm12/ArquitecturaBasadaEnEventosKafka](https://github.com/Juandavm12/ArquitecturaBasadaEnEventosKafka).
- Se incluyen código, configuración, pruebas, reportes y evidencias de los casos documentados. Dependencias instaladas, configuración privada y archivos temporales quedan fuera mediante `.gitignore`.

## Verificar contrato y API

Requisito: Node.js 24 o posterior. Desde la raíz de esta carpeta, en PowerShell:

```powershell
Set-Location .\kafka
npm ci
npm test
```

Son 51 pruebas locales de contrato/API/consumidores (Express y KafkaJS fijados en lockfile), aprobadas con Node 26 del anfitrión y Node 24 aislado. **No sustituyen las pruebas de integración exigidas por el profesor**.

Para comprobar estáticamente el Compose parcial (requiere Docker Compose, pero no el motor en ejecución), desde `kafka/`:

```powershell
node --test test/infra.test.mjs
```

La suite del contrato sigue siendo independiente de Docker. Las dos comprobaciones de infraestructura validan configuración y puertos alternativos; no prueban consumo ni recuperación.

## Arquitectura final: seis contenedores

| Contenedor | Responsabilidad |
| --- | --- |
| `orders-api` | Recibir pedidos y publicar `OrderCreated` |
| `kafka` | Almacenar el log persistente `orders.events` |
| `kafka-ui` | Observar eventos, particiones y grupos |
| `inventory-consumer` | Simular reserva de inventario |
| `billing-consumer` | Simular facturación |
| `notification-consumer` | Simular notificación |

El montaje final tiene seis servicios verificados; la evidencia inicial de broker y UI es una etapa histórica, no una prueba aislada del sistema completo. Para CP-10 se añadió y retiró una instancia temporal de inventario dentro de su mismo grupo, sin modificar esta arquitectura persistente.

[Fuentes técnicas y adaptación del ejemplo Docker](docs/FUENTES_TECNICAS.md).

## Flujo implementado

```text
POST /orders → orders-api → Kafka: orders.events
                              ├─ inventory-consumer (inventory-service-group)
                              ├─ billing-consumer (billing-service-group)
                              └─ notification-consumer (notification-service-group)
```

Los consumidores no intervienen en la respuesta HTTP. Kafka UI permite observar eventos, particiones y grupos; los logs y capturas documentan las ejecuciones reales.
