# Publicación saneada

La publicación inicial fue parcial: **7/10** casos documentados. Esta actualización de entrega alcanza **10/10 técnicamente verificados** mediante [una nueva corrida mínima](docs/verificacion/cierre-minimo-20261101/RESUMEN.md). El informe PDF y capturas GUI adicionales siguen pendientes.

Se reemplazó únicamente una contraseña autogenerada Spring en cada una de las tres copias públicas identificadas en el [manifiesto de publicación](docs/publicacion/manifest.json), por `[REDACTADO_PARA_PUBLICACION]`. Estas copias **no son byteidénticas** a los originales privados, conservados y verificados fuera del repositorio de entrega. En ese saneamiento no cambiaron otros bytes, fechas ni resultados; no hubo nueva corrida, runtime ni captura GUI. La corrida mínima posterior tiene evidencias nuevas separadas y no altera estos originales históricos.

Los SHA históricos siguen refiriéndose a los originales privados: no se reescribieron manifiestos ni validaciones. El manifiesto público es un **overlay**, no una sustitución de evidencia histórica. Referencias afectadas (líneas históricas):

- CP-07: `manifest.json` 128–131 y 272–275; `corrida-01/sources-before.json` 284.
- CP-08: `lectura-runtime/baseline.json` 292, 314 y 338.
- CP-09: `runtime/sources-before.json` 296, 318 y 342; `control/baseline-inicial` 297, 319 y 343.

Los nombres anteriores identifican referencias históricas inmutables, no nuevos archivos. La [validación offline](docs/publicacion/validacion.json) registra integridad, permisos y ausencia del valor retirado sin divulgarlo.
