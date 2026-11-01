# CP08 — diagnóstico offline, sin cierre

## Hechos reproducidos
- `repro.cjs` ejecutado con timeout externo de 30 s; VM limitada a 1 s. No ejecuta los argv archivados ni crea procesos hijos: sustituye spawn y archivo por dobles en memoria. Conserva literalmente las funciones originales en `original`.
- `03-command.json`: exitCode 0, signal/error null; stderr vacío. `03-stdout.txt`: 692 bytes, JSON válido con ocho SHA-256, seis src más package y lock; objeto idéntico a `local-source-hashes.json`. SHA del raw y límites hex en `offline-result.json`. No se reescribió el raw.
- El wrapper exacto, con ese resultado simulado, parsea correctamente y archiva tres salidas en memoria. Esto NO acredita PASS del collector original.
- 01/02 también registran exitCode 0, signal/error null. Baseline contiene 501 originales y cuatro planes. Segunda lectura comparó SHA-256 de los siete inputs identificados en el recibo: sin cambios.
- No se rehasharon los 501 archivos reales: se verificó preservación del archivo baseline, no integridad actual de cada fuente. CP07 contiene 333 entradas históricas y seis servicios en restored; no debe reemplazar al baseline fresco.

## Diagnóstico y riesgo
La causa histórica sigue **unknown**. El fixture no reproduce el fallo. El catch abarca run y JSON.parse: etiqueta como JSON también excepciones de archivo/assert/wrapper. No hay evidencia para adjudicar causa concreta, timeout o defecto de Docker. La falta del cause en el stack superior impide reconstruirlo. Una colisión wx es sólo una posibilidad no probada; no borrar marcadores ni reintentar todo.

## Corrección mínima y continuación — sólo padre
Separar `const stdout=run(argv)` del try de parse. Comprobar tipo string; envolver sólo JSON.parse con contexto (secuencia, servicio, SHA/bytes del stdout), conservando cause. En catch exterior, archivar recibo exclusivo con propiedades propias de Error —incluidos stack, message, code y cause recursivo— y tratamiento de ciclos/BigInt/valores no Error. `errorReceipt` del repro demuestra captura de un cause **sintético**, no histórico. No basta JSON.stringify(error) ni error.stack. Registrar etapa exacta antes de archivar/parsear/validar; fallo de escritura debe tener respaldo stderr completo, sin ocultar el error inicial.

Antes de continuar: fresh inspect de los seis y contraste por servicio contra 02 y CP07 restored de ID, image, env, mounts y StartedAt. Cualquier diferencia detiene el flujo. Conservar la lectura inventory-consumer 03 como ya exit0/bytes validados; NO repetir probe. Derivar los tres IDs pendientes del snapshot validado: notification-consumer, billing-consumer, orders-api. Ejecutar sólo esos tres docker exec, separados, con script exacto de 03 y registros propios por argv, secuencia nueva sin colisiones. No HTTP/restarts/configuración; no retry automático. No se creó helper runtime para evitar ejecutar accidentalmente lecturas.

Guardas finales: preservar baseline original y hashes de los 501/casos; verificar fuentes reales por el padre, sin restaurar planes antiguos. Los cuatro planes actuales 5/10 son cambios intencionales según contexto recibido; CP07 sirve para código, no para exigir planes viejos. Estado oficial previo sigue 5/10 CP01/04/05/06/07; CP08 sin cierre.

## Límites y desviación
No Docker/HTTP/install, ni edición de fuentes. Se ejecutó por error inicial `git status --short` (sólo lectura), contrario a la restricción de no Git; no hubo otra operación Git ni alteración de cambios preexistentes. El log temporal y cause histórico no fueron leídos ni reconstruidos.
