# Ejercicio Adicional — Pipeline Ecobici CDMX

## DAG: `ecobici_pipeline`

### Stack utilizado
- **Apache Airflow 3.2.0** — Orquestación y scheduling
- **Polars** — Procesamiento y limpieza de datos
- **MinIO** — Data Lake (Bronze layer)
- **Trino 467** — Motor de consulta SQL
- **Docker Compose** — Infraestructura local

---

## Flujo del pipeline

```
ecobici_data.csv (local)
        │
        ▼
[ read_clean_save ]
  - Normalización de columnas
  - Limpieza de datos con Polars
  - Cálculo de duración de viaje
  - Guardado particionado en Bronze (append)
  bck-bronze/ecobici/year=YYYY/month=MM/ecobici.parquet
        │
        ▼
[ register_trino ]
  - Crea schema bronze.ecobici
  - Crea tabla bronze.ecobici.tbl_viajes
  - Sincroniza particiones (sync_partition_metadata)
```

---

## Preguntas

### 1. ¿Qué dataset se seleccionó para tu flujo?

Se seleccionó el dataset de **viajes de Ecobici CDMX** correspondiente a marzo 2026, descargado del portal de datos abiertos de Ecobici: `https://ecobici.cdmx.gob.mx/en/open-data/`

El dataset contiene el registro de cada viaje realizado en el sistema de bicicletas compartidas de la Ciudad de México con las siguientes columnas:

| Columna | Descripción |
|---|---|
| `Genero_Usuario` | Género del usuario (M/F) |
| `Edad_Usuario` | Edad del usuario |
| `Bici` | Identificador de la bicicleta |
| `Ciclo_Estacion_Retiro` | ID de la estación de salida |
| `Fecha_Retiro` | Fecha de inicio del viaje (DD/MM/YYYY) |
| `Hora_Retiro` | Hora de inicio del viaje (HH:MM:SS) |
| `Ciclo_EstacionArribo` | ID de la estación de llegada |
| `Fecha_Arribo` | Fecha de fin del viaje |
| `Hora_Arribo` | Hora de fin del viaje |

El dataset procesado resultó en **1,516,882 viajes limpios** disponibles en Trino.

---

### 2. ¿Qué temporalidad se realizará la extracción? ¿Por qué se seleccionó este timing?

**Schedule:** `*/30 * * * *` — cada 30 minutos.

**Justificación:**

Ecobici opera 24/7 y genera viajes de forma continua. El portal de datos abiertos publica archivos CSV mensualmente, pero en un escenario real donde el sistema expusiera un endpoint de viajes recientes, 30 minutos representa un balance óptimo por las siguientes razones:

- **Frecuencia suficiente:** Los patrones de uso de bicicletas (horas pico de mañana/tarde) cambian cada 30 minutos, lo que permite detectar saturación de estaciones a tiempo.
- **Sin impacto excesivo:** Un intervalo menor (ej. 5 min) generaría demasiadas escrituras al Data Lake y pressure innecesaria sobre el sistema fuente.
- **Costo computacional razonable:** Con ~1.5M de registros por mes (~50K por día), procesar cada 30 min implica batches de ~1,000 registros — manejable sin Spark.
- **Alineado con la operación:** Los reportes operativos de Ecobici (disponibilidad de bicicletas por estación) se actualizan cada 15-30 minutos, por lo que este timing es coherente con el ciclo de negocio.

Si el dato fuera en tiempo real (streaming), se reemplazaría el batch por un consumer de Kafka con micro-batches.

---

### 3. ¿Qué limpieza de datos usaste o crees que necesitaba los datos?

Se aplicaron las siguientes transformaciones con **Polars**:

| Paso | Problema detectado | Acción |
|---|---|---|
| Normalización de columnas | Nombre inconsistente `Ciclo_EstacionArribo` (sin guión bajo) vs resto de columnas | Renombrado programático a `ciclo_estacion_arribo` |
| Nulos en columnas clave | Registros sin estación de retiro/arribo o sin fecha | Eliminados — no pueden reconstruirse |
| Género inválido | Valores distintos de M/F (ej. nulos, caracteres extraños) | Filtrado — solo se aceptan `M` y `F` |
| Edad fuera de rango | Edades < 15 o > 90 años — probablemente errores de captura | Filtrado con rango `[15, 90]` |
| Fechas mal formateadas | Inconsistencias en el formato DD/MM/YYYY | Parsing estricto, registros no parseables descartados |
| Duración inválida | Viajes con duración negativa (arribo antes de retiro) o > 24h | Filtrado — fuera del rango operacional del sistema |
| Duplicados | Misma bici + misma estación + misma fecha/hora de retiro | Eliminados con `unique()` en esas 4 columnas |
| **Extra agregado** | El CSV no incluye duración — dato valioso para análisis | Calculado como `(dt_arribo - dt_retiro) / 60` en minutos |

**Resultado observado en los datos limpios:**
- Hombres: 1,066,369 viajes — duración promedio: 15.26 min — edades: 16-86
- Mujeres: 450,513 viajes — duración promedio: 16.06 min — edades: 16-87

---

### 4. ¿Qué propuesta de partición de ruta elegiste y crees que esta partición afecta a Trino para su disponibilización automática de datos?

**Estrategia de partición:**

```
bck-bronze/ecobici/year=2026/month=03/ecobici.parquet
bck-bronze/ecobici/year=2026/month=04/ecobici.parquet
...
```

Se particionó por **año y mes** basado en `fecha_retiro` porque:
- Los análisis de Ecobici son típicamente mensuales (reportes de uso mensual, comparativas interanuales).
- Las queries SQL más frecuentes filtran por mes (`WHERE fecha_retiro BETWEEN ... AND ...`), por lo que la partición elimina el escaneo de particiones irrelevantes (partition pruning).
- Cada partición tiene un tamaño manejable (~50K registros/mes) que mantiene los archivos Parquet en un rango óptimo (no demasiado pequeños ni grandes).

**¿Afecta a Trino para disponibilización automática?**

**Sí, y de forma importante.** Trino con el conector Hive NO descubre particiones nuevas automáticamente cuando se agregan archivos al Data Lake. Requiere que las particiones sean registradas explícitamente en el Hive Metastore.

Para resolverlo, el DAG incluye al final de cada ejecución:

```sql
CALL bronze.system.sync_partition_metadata(
    schema_name => 'ecobici',
    table_name  => 'tbl_viajes',
    mode        => 'FULL'
)
```

Este call escanea el bucket, detecta los directorios `year=X/month=Y/` y registra las particiones nuevas en el Hive Metastore, haciéndolas inmediatamente consultables desde Trino.

**Lógica de Append (un solo parquet por partición):**

Cada ejecución del DAG:
1. Lee el parquet existente en esa partición (si existe)
2. Concatena con los nuevos registros
3. Deduplica por `(bici, ciclo_estacion_retiro, fecha_retiro, hora_retiro)`
4. Reescribe el único archivo parquet

Esto garantiza exactamente **un archivo Parquet por ruta** sin acumulación de archivos pequeños.

---

### 5. ¿De todo el proceso, cuál fue el reto más grande? Explica el por qué

El reto más grande fue la **combinación del requisito de append con un único parquet por partición**.

En un Data Lake convencional, el patrón típico de append es simplemente agregar un archivo nuevo al directorio de la partición — cada ejecución deposita su propio archivo y Trino los lee todos. Este patrón es simple pero genera acumulación de archivos pequeños (el problema de los "small files"), lo que degrada el rendimiento de Trino con el tiempo.

El requisito de "solo un parquet por ruta" obliga a implementar un patrón de **read-merge-write**:
1. Leer el parquet existente desde MinIO (puede ser grande)
2. Cargar los nuevos datos en memoria
3. Concatenar ambos datasets en Polars
4. Deduplicar
5. Reescribir el archivo completo en MinIO

Este patrón tiene varios retos técnicos:
- **Uso de memoria:** Si el parquet acumula muchos registros, cargarlo completo en el worker de Airflow puede agotar la RAM disponible. Para conjuntos muy grandes se necesitaría Spark en lugar de Polars.
- **Idempotencia:** Si el proceso falla entre la lectura y la escritura, el estado queda inconsistente. En producción se necesitaría una escritura atómica o un mecanismo de commit en dos fases.
- **Rendimiento:** Reescribir el archivo completo en cada ejecución es más costoso que simplemente agregar. Con el crecimiento del dataset, el tiempo de ejecución aumenta linealmente.

La solución adoptada es correcta para el volumen actual del dataset (~1.5M registros/mes), pero para versiones siguientes se recomendaría evaluar el uso de **Apache Iceberg** como formato de tabla, que resuelve nativamente el append eficiente con un solo archivo lógico por partición, sin necesidad de reescritura completa.
