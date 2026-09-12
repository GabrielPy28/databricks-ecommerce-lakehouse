"""Columnas de linaje que Bronze añade a cada fila.

Se declaran aquí, y no en el módulo de ingesta, porque los tests del contrato
comprueban que ninguna entidad de negocio use estos nombres: una colisión
sobrescribiría un dato real con metadatos de ingesta, en silencio.
"""

from __future__ import annotations

# Momento en que Bronze escribió la fila.
INGESTION_TIMESTAMP = "_ingestion_timestamp"

# Fichero del que salió. Imprescindible para rastrear un dato hasta su origen
# cuando algo no cuadra.
SOURCE_FILE = "_source_file"

# Lote lógico al que pertenece. Permite reprocesar o descartar un lote entero.
BATCH_ID = "_batch_id"

# Ejecución concreta del pipeline que la escribió. Enlaza la fila con su
# entrada en `ops.pipeline_runs` y con los resultados de calidad.
PIPELINE_RUN_ID = "_pipeline_run_id"

LINEAGE_COLUMNS: tuple[str, ...] = (
    INGESTION_TIMESTAMP,
    SOURCE_FILE,
    BATCH_ID,
    PIPELINE_RUN_ID,
)
