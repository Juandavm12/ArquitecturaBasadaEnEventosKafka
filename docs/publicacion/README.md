# Publicación saneada y hashes históricos

Tres copias públicas contienen `[REDACTADO_PARA_PUBLICACION]` en lugar de una contraseña autogenerada Spring. El [manifiesto de publicación](manifest.json) identifica las copias: no son byteidénticas a los originales privados conservados y verificados fuera del repositorio de entrega.

En ese saneamiento solo cambió ese valor, no fechas ni resultados; no hubo nueva corrida, runtime ni captura GUI. La [corrida mínima posterior](../verificacion/cierre-minimo-20261101/RESUMEN.md) conserva sus evidencias separadamente: CP-10/11/12 usan vistas derivadas de CLI/JSON, no capturas GUI.

Los SHA históricos corresponden a los originales privados y no deben recalcularse sobre las copias saneadas para sustituirlos. El manifiesto público es un **overlay**, no una sustitución de evidencia histórica. Referencias históricas afectadas:

- CP-07: `manifest.json` 128–131 y 272–275; `corrida-01/sources-before.json` 284.
- CP-08: `lectura-runtime/baseline.json` 292, 314 y 338.
- CP-09: `runtime/sources-before.json` 296, 318 y 342; `control/baseline-inicial` 297, 319 y 343.

Los nombres y líneas anteriores son referencias históricas, no archivos nuevos. La [validación offline](validacion.json) registra integridad, permisos y ausencia del valor retirado sin divulgarlo. `manifest.json` y `validacion.json` se conservan intactos; cualquier referencia a `PUBLICACION.md` identifica la ruta antigua al momento de la validación, no la ubicación de esta guía actual.
