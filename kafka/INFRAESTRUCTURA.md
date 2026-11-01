# Infraestructura del laboratorio

Seguir [Ejecución del sistema completo](../docs/EJECUCION.md), no el arranque parcial de dos servicios. Compose incluye API, Kafka KRaft 3.9.1, UI de solo lectura y tres consumidores. La API crea el topic idempotentemente al iniciar.

El broker usa un nodo combinado, quorum `1@kafka:9093`, listeners internos `kafka:9092` y externo configurable en loopback, dos particiones y RF1. Controller 9093 no se publica. Heap configurado: Kafka 512 MiB y UI 256 MiB; no son límites totales de memoria. UI sin TLS/autenticación: solo laboratorio local.

Validación estática desde esta carpeta: `node --test test/infra.test.mjs` y `docker compose config --format json`. No arrancan servicios. [Resultados históricos de infraestructura](../docs/verificacion/infraestructura/RESUMEN.md) · [Fuentes de la adaptación](../docs/FUENTES_TECNICAS.md).

Este enlace de compatibilidad reemplaza las instrucciones de fase 2/6 sin modificar sus evidencias históricas.
