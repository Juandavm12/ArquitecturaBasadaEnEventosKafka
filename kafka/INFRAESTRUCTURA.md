# Infraestructura parcial: Kafka + UI

Esta guía describe la fase inicial de **2 de los 6 servicios finales**. El Compose
actual incluye los seis servicios: seguir también [API.md](API.md) y
[CONSUMIDORES_RUNTIME.md](CONSUMIDORES_RUNTIME.md) para el montaje completo.
No cierra ningún caso de prueba (CP) de integración. El broker y la UI fueron
verificados en runtime: [resultados y límites](../docs/verificacion/infraestructura/RESUMEN.md).
RabbitMQ permanece fuera de alcance; no se modifica ni se inicia.

## Validación estática reproducible

Desde esta carpeta (o desde `kafka/` después de copiar):

```powershell
node --test test/infra.test.mjs
docker compose config --format json
```

Solo se interpreta Compose: no requiere daemon, descarga ni arranque. Los registros
`../docs/verificacion/infraestructura/estatica-staging/red.log` y
`../docs/verificacion/infraestructura/estatica-staging/green.log` conservan comando, código,
stdout y stderr de las invocaciones, incluidos los puertos alternativos.

## Arranque y comprobación reproducibles

Comandos PowerShell desde `kafka/`, con Docker disponible. Las operaciones equivalentes
fueron verificadas desde Linux/WSL; no se afirma ejecución nativa Windows:

```powershell
# Opcional: cambiar ambos puertos antes de arrancar.
$env:KAFKA_EXTERNAL_PORT = "29092"
$env:KAFKA_UI_PORT = "8080"
docker compose -p qa-taller-integracion-kafka up -d --wait --wait-timeout 180 kafka kafka-ui
docker compose -p qa-taller-integracion-kafka exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic orders.events --partitions 2 --replication-factor 1
docker compose -p qa-taller-integracion-kafka exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders.events
Invoke-WebRequest -Uri "http://127.0.0.1:$($env:KAFKA_UI_PORT)" -UseBasicParsing
Start-Process "http://127.0.0.1:$($env:KAFKA_UI_PORT)"
```

Comprobar en `describe`: dos particiones y factor de replicación uno. Si el tópico
ya existía, `--if-not-exists` no corrige su configuración: revisar el resultado.
La UI es de consulta (`READONLY=true`); la creación explícita usa CLI del broker.
`--wait` comprueba salud del broker y arranque de UI, no salud HTTP de UI.
No se añade healthcheck HTTP porque no se verificaron herramientas dentro de esa
imagen. La petición PowerShell es una comprobación manual adicional.

## Decisiones y límites

- Se adapta el ejemplo con `bitnamilegacy` 3.7 a la imagen oficial versionada
  `apache/kafka:3.9.1`, usando `KAFKA_*`, no `KAFKA_CFG_*`. No se atribuyen
  vulnerabilidades. Las etiquetas descargadas y sus digests constan en la evidencia.
- KRaft combinado, nodo 1 y quorum `1@kafka:9093`: laboratorio sin alta disponibilidad.
  Réplicas y mínimo ISR son uno; dos particiones por defecto y autocreación apagada.
- Clientes internos usan `kafka:9092`; el host usa `localhost` y el puerto externo
  configurable. El puerto contenedor externo sigue siendo 29092. Controller 9093
  no se publica. Ambos puertos publicados se restringen a `127.0.0.1`.
- Volumen nombrado `kafka-data`, mismo directorio `/var/lib/kafka/data` en montaje
  y configuración; ID de clúster fijo para este laboratorio. No se exige root.
- Heap Kafka 512 MiB, UI 256 MiB; no son límites totales de memoria del contenedor.
  Retención temporal explícita: 168 horas, no borrado instantáneo garantizado.
- UI sin autenticación ni TLS, solo laboratorio local. Modo lectura y configuración
  dinámica desactivada reducen cambios accidentales; no sustituyen autorización
  en el broker ni hacen seguro publicar estos servicios en otras interfaces.
- Verificados: descarga de ambas etiquetas, arranque del broker con su volumen,
  clúster online desde UI y tópico de dos particiones, con capturas GUI reales
  aportadas por el usuario y conservadas en la evidencia. Pendientes: prueba
  de persistencia con eventos y extensión a seis servicios.

Referencia oficial de la adaptación (no evidencia de ejecución):
https://raw.githubusercontent.com/apache/kafka/3.9.1/docker/examples/README.md
