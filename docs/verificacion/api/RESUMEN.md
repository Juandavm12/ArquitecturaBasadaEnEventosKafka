# API Kafka — primera publicación verificada

Registro histórico de la fase de publicación (3/6). Estado posterior: [seis servicios y tres grupos consumidores verificados](../consumidores/RESUMEN.md).

**Resultado:** API y broker saludables, UI activa; **3/6 servicios**, sin consumidores. Un único pedido obtuvo HTTP 202 y quedó almacenado en `orders.events`. Kafka sigue **0/10 reportes formales cerrados**; captura GUI del primer evento recibida, correlacionada y archivada sin modificación.

## Pedido real — no repetir para capturar

- `orderId`: **ORD-f2035454-c380-4e9f-b682-b48ef72ca568**.
- `eventId`: `657b734d-55ed-4d10-ba08-5277b9eba6b2`; `correlationId`: `cdb824fe-f331-4a6d-8c84-2dbb89ba6566`.
- Datos: Pedro, Libro de arquitectura, cantidad 1, precio y total 85000.
- HTTP 202 en **0.381403 s**, publicación registrada el **2026-10-27 12:19:38.456 UTC**; no es un SLA.
- Partición **1**, offset **0**; offsets finales por partición **0/1**, desde **0/0**.
- [Resultado correlacionado](primera-publicacion/corrida-01/result.json), [comandos/exitcodes/tiempos](primera-publicacion/corrida-01/commands.json), [entrada](primera-publicacion/corrida-01/request.json), [respuesta](primera-publicacion/corrida-01/response.json), [cabeceras HTTP](primera-publicacion/corrida-01/response-headers.txt).
- [Log API](primera-publicacion/corrida-01/api-logs-stdout.txt) y [dump nativo del segmento del broker](primera-publicacion/corrida-01/broker-native-log-stdout.txt) contienen los mismos IDs. El segundo es una lectura del almacenamiento real, no stdout del servicio ni una captura GUI.

[Captura GUI original del evento](primera-publicacion/corrida-01/capturas/01-order-created-kafka-ui.png) · [SHA-256, comprobaciones y límites](primera-publicacion/corrida-01/capturas/01-order-created-kafka-ui.json). Se leen el JSON completo, los mismos tres IDs y partición 1/offset 0. El nombre del tópico no aparece en el recorte: se correlaciona con `orders.events` mediante el registro nativo. «1 messages consumed» corresponde al visor UI, **no a los tres consumidores funcionales**, aún no implementados. No se repitió el POST; `result.json` mantiene su estado histórico anterior a la captura.

## Verificación reproducible

| Comprobación | Evidencia y resultado |
| --- | --- |
| TDD API en staging | [RED](staging-reanudacion-01/red/command.json) exit 1 antes del módulo; [GREEN](staging-reanudacion-01/green/command.json) exit 0, 29/29 contrato + HTTP locales |
| Instalación con lock | [npm ci](integrada/npm-ci/command.json) exit 0, auditoría npm 0 vulnerabilidades observadas |
| Pruebas integradas host Node 26.8.1 | [Resultados corregidos](integrada/tests-correccion-ruta/commands.json): 29/29 contrato/API y 2/2 Compose, exit 0 |
| Mismas pruebas en Node 24 aislado | [Resultado](integrada/tests-node24/command.json): 29/29, exit 0; contenedor efímero sin red externa, ningún pedido Kafka |
| Imagen y arranque real | [Digest Node](imagen-node-reanudacion-01/inspect-stdout.txt), [Node efectivo 24.22.1](runtime-arranque/node-version.json), [build/up/health](runtime-arranque/commands.json): comandos exit 0 |

Seguir [API.md](../../../kafka/API.md) desde `kafka/`, con proyecto `qa-taller-integracion-kafka`. Linux/WSL verificado; Windows nativo no certificado. HTTP 202 confirma publicación, no procesamiento ni exactly-once. La recuperación con eventos y el cierre graceful de la API no se ensayaron en esta unidad.

## Incidentes y límites conservados

- Instalación anterior interrumpida: staging `trabajos/entrega-kafka/api/` del semestre se preservó con dependencias y registros vacíos. El staging nuevo y sus registros son independientes; [npm nuevo](staging-reanudacion-01/npm/command.json) exit 0.
- La primera prueba integrada de Compose falló por exigir el nombre de la carpeta de staging: [fallo original](integrada/tests/commands.json). Se corrigió a contexto de build resuelto desde la carpeta actual; la corrida corregida anterior pasó.
- El harness de arranque terminó **exit 1** por comparar el orden de los mounts, aunque sus siete comandos terminaron exit 0. [Log original](runtime-arranque/harness-original-log.txt) y [validación offline corregida](runtime-arranque/validacion-montajes-corregida.json): mismos IDs y montajes ordenados por destino; un volumen sintético diferente es rechazado. No se repitió el arranque por ese fallo.
- Los logs originales incluyen `TimeoutNegativeWarning` y un error de respuesta `CreateTopics`. KafkaJS contempla `TOPIC_ALREADY_EXISTS` como retorno `false`; sus temporizadores también pueden calcular demoras negativas. Son explicaciones compatibles con el código local, no causas certificadas sin stack/código de error detallado. No se ocultaron avisos; health y publicación sí se verificaron.
- LSP no confirmó cobertura completa y señaló el uso de `console.log` del logger JSON para Docker. No se declara lint limpio. Las pruebas de desarrollo no sustituyen los diez CP ni sus capturas.
