# Arquitectura basada en eventos con Kafka

Prueba de concepto del taller de Pruebas de Integración de Aseguramiento de la Calidad. **Entrega parcial: 7/10 casos documentados (CP-01, CP-04, CP-05, CP-06, CP-07, CP-08 y CP-09).** CP-10, CP-11, CP-12 y el informe PDF global están pendientes; no se presentan como completados.

## Estado

- [Publicación saneada y manifiestos históricos](PUBLICACION.md): tres redacciones identificadas; originales privados conservados.

- Guía completa y anexo 2 revisados; requisitos en [PLAN_KAFKA.md](PLAN_KAFKA.md).
- RabbitMQ permanece separado; no se copia su código ni sus resultados como evidencia Kafka.
- Primera unidad: contrato `OrderCreated` y validación offline.
- Infraestructura broker + UI verificada: broker healthy, UI online y `orders.events` con dos particiones. [Comandos PowerShell](kafka/INFRAESTRUCTURA.md) · [resultados reales](docs/verificacion/infraestructura/RESUMEN.md). Todavía no equivale al sistema completo.
- API verificada: HTTP 202, un `OrderCreated` en partición 1/offset 0, mismo pedido en API y log nativo del broker. [Evidencia y límites](docs/verificacion/api/RESUMEN.md). Actualmente **6/6 servicios**, con consumo real en tres grupos: [resultados](docs/verificacion/consumidores/RESUMEN.md).
- Captura GUI del primer evento recibida y correlacionada, original y SHA-256 en la evidencia anterior.
- [CP-05 — Consumo de evento Kafka: Cumple](docs/casos/CP05/reporteCP05.md). Tres consumidores recibieron y procesaron el mismo evento; logs del 27 de octubre y capturas registradas el 31 de octubre de 2026. Capturas de grupos y de los tres consumidores conservadas con SHA-256.
- [CP-04 — Publicación de evento Kafka: Cumple](docs/casos/CP04/reporteCP04.md). Corrida del 31 de octubre: Postman CLI 202/370 ms, cinco verificaciones aprobadas y el mismo evento en API, almacenamiento nativo y Kafka UI; partición 0/offset 0. Fuentes y capturas conservadas con SHA-256.
- [CP-01 — Creación de pedido vía API: Cumple](docs/casos/CP01/reporteCP01.md). Recepción y respuesta verificadas con la misma petición del 31 de octubre utilizada en CP-04; sin otro POST.
- [CP-06 — Trazabilidad del flujo: Cumple](docs/casos/CP06/reporteCP06.md). Mismos identificadores en API, log persistente del broker y las tres fases de cada consumidor; evidencia histórica del 27 de octubre.
- [CP-07 — Responsive: Cumple](docs/casos/CP07/reporteCP07.md). HTTP 202/239 ms; respuesta observada 2991 ms antes del fin de inventario con demora experimental de 3000 ms, no SLA.
- [CP-08 — Message-driven: Cumple](docs/casos/CP08/reporteCP08.md). Flujo auditado mediado por Kafka, evidencia CP-07 compartida y auditoría de hashes posterior; sin otra publicación.
- [CP-09 — Resilient: Cumple](docs/casos/CP09/reporteCP09.md), parada controlada de inventario, HTTP 202 y continuidad de los otros dos consumidores; restauración verificada. CP-10/11/12 siguen pendientes; sin nueva ejecución para este cierre.
- Pendientes: tres reportes de integración (CP-10/11/12), sus escenarios/evidencias faltantes y el PDF.
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

El montaje se verifica por etapas; levantar inicialmente broker y UI no completa esta arquitectura. Para CP-10 se añadirá temporalmente otra instancia de una misma responsabilidad dentro de su grupo.

[Fuentes técnicas y adaptación del ejemplo Docker](docs/FUENTES_TECNICAS.md).

## Flujo que se implementará

```text
POST /orders → orders-api → Kafka: orders.events
                              ├─ inventory-consumer (inventory-service-group)
                              ├─ billing-consumer (billing-service-group)
                              └─ notification-consumer (notification-service-group)
```

Los consumidores no intervienen en la respuesta HTTP. Kafka UI permitirá observar eventos, particiones y grupos; los logs y capturas documentarán las ejecuciones reales.
