# Bitacora

Este doc es para hacer tracking de todos los issues y decisiones de diseño que se tomen al rededor del proyecto. 

## 2026-09-23 · Ingesta (requisito 1)

Reescritura de `ingest.ipynb`.

Issues encontrados en la primera versión:

- `saveAsTable("ais")` sin calificar escribía en `workspace.default.ais` en vez de `ocean_watch.raw.ais`; por eso la tabla crecía en cada ejecución.
- Las rutas mezclaban `ais_2023_06_0{DAY}` y `ais_2023_06_{DAY}`, y los `try/except` ocultaban los errores de lectura.
- `Heading` estaba declarado como `IntegerType`, pero NOAA lo publica como `511.0`: en modo `PERMISSIVE` toda la columna habría quedado en `null` sin error.
- El `CREATE TABLE` no coincidía con el esquema de lectura (`NavStatus` vs `Status`, tipos distintos).
- Solo se descargaban 3 de los 7 días y no había verificación de integridad.

Decisiones:

- Descarga en streaming con reintentos y backoff exponencial (10, 20, 40, 80 s); los HTTP 4xx no se reintentan.
- Integridad: bytes recibidos vs `Content-Length`, CRC-32 con `ZipFile.testzip()` y tamaño del CSV extraído vs el declarado en el zip. NOAA no publica checksums; se guarda el SHA-256 del zip en un manifiesto por día (`_manifest/`) para trazabilidad.
- Idempotencia: un día con manifiesto y CSV del tamaño esperado no se vuelve a descargar.
- Se valida el encabezado de cada CSV porque Spark no lo compara contra el esquema explícito.
- Columna `_corrupt_record` para contar filas que no se pueden convertir al tipo declarado (insumo para la exploración de calidad).
- La tabla raw queda sin particionar a propósito: es la línea base para el experimento de almacenamiento (requisito 4).
- Verificado localmente con el día 1: 8.808.904 filas, todas convierten a los tipos declarados.
