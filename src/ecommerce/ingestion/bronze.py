"""Escritura en Bronze.

Principio de la capa: **preservar lo que llegó**. Ninguna regla de negocio,
ningún tipado, ninguna fila descartada. Lo único que se añade son metadatos de
linaje, que no alteran el dato: lo explican.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecommerce.schemas import metadata

# El lote va en la ruta del fichero: `.../<entidad>/batch_NNN/part-*.parquet`.
PATRON_LOTE = re.compile(r"/(batch_\d+)/")


def batch_from_path(ruta: str | None) -> str | None:
    """Extrae el lote de la ruta de un fichero. `None` si no lo lleva.

    Devolver `None` en lugar de un valor por defecto es deliberado: una etiqueta
    inventada parecería buena, y `build_gold` recalcularía una ventana
    equivocada creyéndosela.
    """
    if not ruta:
        return None
    encontrado = PATRON_LOTE.search(ruta)
    return encontrado.group(1) if encontrado else None


# Versión SQL del mismo patrón, para aplicarlo sobre la columna sin recurrir a
# una UDF de Python —que en Spark obliga a serializar fila por fila.
_REGEX_SQL = r".*/(batch_[0-9]+)/.*"


def add_lineage(df: DataFrame, *, run_id: str) -> DataFrame:
    """Añade las cuatro columnas de linaje a un DataFrame leído de ficheros.

    `_source_file` sale de `_metadata.file_path`, una columna oculta que Spark
    expone en las fuentes basadas en ficheros. Es lo que permite rastrear una
    fila concreta hasta el fichero del que salió cuando algo no cuadra, y por
    eso el DataFrame tiene que provenir de ficheros y no de memoria.

    **`_batch_id` se deduce de esa misma ruta, no de un parámetro.** Auto Loader
    ingiere todos los ficheros nuevos del directorio que vigila, sin saber nada
    del parámetro con el que se invocó el job: etiquetar con el parámetro
    produce etiquetas que mienten. Ocurrió de verdad —dos lotes distintos
    quedaron marcados como `batch_000`— y no rompió nada, que es lo peligroso:
    `build_gold` deriva de esta columna los días que recalcula, así que una
    etiqueta equivocada deja Gold desactualizado en silencio.

    El lote es una propiedad del dato, no de la invocación.
    """
    ruta = F.col("_metadata.file_path")
    return (
        df.withColumn(metadata.INGESTION_TIMESTAMP, F.current_timestamp())
        .withColumn(metadata.SOURCE_FILE, ruta)
        .withColumn(
            metadata.BATCH_ID,
            # `regexp_extract` devuelve cadena vacía cuando no hay coincidencia;
            # se convierte a nulo para que "sin lote" sea distinguible de un
            # lote llamado "".
            F.nullif(F.regexp_extract(ruta, _REGEX_SQL, 1), F.lit("")),
        )
        .withColumn(metadata.PIPELINE_RUN_ID, F.lit(run_id))
    )
