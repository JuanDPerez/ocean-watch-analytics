# ocean-watch-analytics

Proyecto del curso Soluciones Intensivas en Datos (MINE 4213, Uniandes 2026-20). OceanWatch Analytics: el lakehouse del tráfico marítimo, construido en Databricks Free Edition con datos AIS de NOAA.

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `ingest.ipynb` | Notebook único de la Entrega 1: ingesta, exploración, preguntas de negocio, almacenamiento y gobernanza |
| `bitacora.md` | Registro de avance, issues y decisiones por clase |
| `SID26.2.ProyectoEntrega1.pdf` | Enunciado de la Entrega 1 |

## Cómo ejecutar

1. Importar el repositorio en Databricks (Git folder) o subir `ingest.ipynb`.
2. Ejecutar el notebook completo en cómputo serverless. La descarga de los 7 días toma entre 20 y 30 minutos.
3. Ejecuciones posteriores omiten los días ya descargados y verificados.

## Datos

- **Fuente:** [NOAA Marine Cadastre, AIS Vessel Traffic](https://hub.marinecadastre.gov/pages/vesseltraffic). Un zip por día con un CSV de posiciones.
- **Corpus:** 1 al 7 de junio de 2023, `https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_DD.zip`. Unos 330 MB comprimidos y 900 MB descomprimidos por día; el día 1 tiene 8.808.904 filas.
- **Puertos:** World Port Index (pendiente de cargar para la pregunta 3d).

## Objetos en Unity Catalog

| Objeto | Contenido |
|---|---|
| `ocean_watch` | Catálogo del proyecto |
| `ocean_watch.raw` | Datos tal como se publican en la fuente, sin limpieza |
| `ocean_watch.raw.ais_raw` (Volume) | `csv/`: los 7 CSV descomprimidos. `_manifest/`: un JSON por día con la evidencia de integridad |
| `ocean_watch.raw.ais` | Tabla Delta con todas las posiciones de la semana |

## 1. Ingesta

### Flujo

| Paso | Qué hace | Evidencia que deja |
|---|---|---|
| 1.1 | Crea catálogo, esquema y Volume | Objetos en Unity Catalog con comentarios |
| 1.2 | Descarga con reintentos, verifica integridad y descomprime en el Volume | Manifiesto JSON por día y tabla resumen |
| 1.3 | Valida el encabezado de cada CSV | `assert` por archivo |
| 1.4 | Lee con esquema explícito y escribe `ocean_watch.raw.ais` | Tabla Delta |
| 1.5 | Agrega comentarios y propiedades a la tabla | `DESCRIBE TABLE EXTENDED` |
| 1.6 | Reconcilia filas del CSV contra filas de la tabla | Tabla de reconciliación por archivo |
| 1.7 | Limpieza opcional de artefactos de la primera versión | — |

### Descarga y reintentos

- La descarga se hace en streaming (bloques de 8 MB) para no cargar el zip completo en memoria.
- Hasta 5 intentos con backoff exponencial: 10, 20, 40 y 80 segundos entre intentos. Un intento se aborta si pasan 120 segundos sin recibir datos.
- Los errores HTTP 4xx (salvo 429) no se reintentan, porque indican una URL equivocada y no un fallo transitorio.
- Los nombres de archivo usan el día con dos dígitos (`AIS_2023_06_01`) y se construyen en un solo lugar para que las rutas no se desalineen.

### Verificación de integridad

Se hace dentro de cada intento; si alguna falla, el archivo se vuelve a descargar.

1. Los bytes recibidos deben coincidir con el `Content-Length` del servidor (detecta descargas truncadas).
2. El zip debe contener exactamente el CSV esperado.
3. `ZipFile.testzip()` recalcula el CRC-32 del contenido (detecta corrupción).
4. Tras descomprimir, el tamaño del CSV en el Volume debe coincidir con el declarado en el zip.

NOAA no publica checksums, así que se calcula el SHA-256 del zip y se guarda en el manifiesto junto con el ETag, los tamaños, el CRC y el número de intentos. Esto permite detectar si la fuente cambia en el futuro.

### Idempotencia

El zip se descarga al disco local (`/tmp`), se descomprime directamente en el Volume y se borra. El manifiesto se escribe al final, solo si todo salió bien: es la marca de "día ingerido". Si existe el manifiesto y el CSV tiene el tamaño esperado, el día se omite.

### Validación del encabezado

Con `header=True` y un esquema explícito, Spark no compara los nombres del encabezado contra el esquema (`enforceSchema=true` por defecto). Si NOAA cambiara el orden de las columnas, los valores quedarían en la columna equivocada sin error. Por eso se compara el encabezado de cada archivo con las 17 columnas esperadas antes de leer.

### Esquema

Todas las columnas son `nullable`: son lecturas de sensores con datos imperfectos, y en la capa raw no se rechaza ninguna fila. Las limpiezas se hacen en la Entrega 2.

| Columna | Tipo | Justificación |
|---|---|---|
| `MMSI` | `STRING` | Identificador, no cantidad. Como texto se puede verificar que tenga 9 dígitos (hay valores de 7 como `9110192`) |
| `BaseDateTime` | `TIMESTAMP` | Formato `yyyy-MM-dd'T'HH:mm:ss`, en UTC. La sesión se fija en UTC |
| `LAT`, `LON` | `DOUBLE` | Grados decimales, rangos válidos [-90, 90] y [-180, 180] |
| `SOG` | `DOUBLE` | Velocidad sobre el fondo, nudos |
| `COG` | `DOUBLE` | Rumbo sobre el fondo, grados |
| `Heading` | `DOUBLE` | Viene como `511.0`. Con `INT`, Spark en modo `PERMISSIVE` dejaría toda la columna en `null` sin avisar. 511 = no disponible |
| `VesselName`, `CallSign` | `STRING` | Texto libre, con frecuencia vacío |
| `IMO` | `STRING` | Viene como `IMO9074729`, vacío o `IMO0000000` |
| `VesselType` | `INT` | Código AIS de tipo de buque (30 pesca, 7x carga, 8x tanquero…) |
| `Status` | `INT` | Estado de navegación, 0 a 15 |
| `Length`, `Width`, `Draft` | `DOUBLE` | Metros |
| `Cargo` | `INT` | Código de tipo de carga, casi siempre vacío |
| `TransceiverClass` | `STRING` | `A` o `B` |
| `_corrupt_record` | `STRING` | Línea original cuando algún campo no se pudo convertir al tipo declarado |

Se verificó localmente que las 8.808.904 filas del día 1 convierten sin errores a estos tipos.

La tabla agrega tres columnas de linaje: `source_file` (desde `_metadata.file_name`, que reemplaza a `input_file_name()` en Unity Catalog), `day` (fecha UTC de `BaseDateTime`) e `ingested_at`.

### Escritura

- Los 7 CSV se leen en una sola lectura (`csv/*.csv`) para que Spark paralelice entre archivos.
- Se escribe con nombre totalmente calificado y `mode("overwrite")`: cada ejecución reemplaza la tabla completa. La primera versión usaba `saveAsTable("ais")`, que resolvía a `workspace.default.ais` y crecía en cada ejecución.
- La tabla queda sin particionar ni clusterizar a propósito: es la línea base del experimento de almacenamiento (sección 4).

### Reconciliación

Por cada archivo se cuentan las líneas no vacías del CSV menos el encabezado y se comparan con las filas cargadas; si no coinciden, el notebook falla. También se reportan, como insumo para la exploración de calidad:

- `corrupt_rows`: filas con algún campo que no se pudo convertir.
- `rows_outside_file_day`: filas cuyo `BaseDateTime` no cae en el día del nombre del archivo.

## 2. Exploración y perfilamiento

Pendiente.

## 3. Preguntas de negocio

Pendiente.

## 4. Almacenamiento óptimo

Pendiente.

## 5. Gobernanza

Catálogo, esquema `raw`, Volume y tabla `ais` creados con comentarios y propiedades. Pendiente: esquemas y tablas derivadas.
