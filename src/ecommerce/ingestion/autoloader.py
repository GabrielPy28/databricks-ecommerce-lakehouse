"""Configuración de Auto Loader.

Auto Loader (`cloudFiles`) lleva su propio registro de qué ficheros ya procesó.
Es lo que da idempotencia a Bronze sin escribir una línea de lógica: volver a
ejecutar la ingesta sobre un lote ya ingerido no añade nada.

Las opciones se construyen en una función pura para poder comprobarlas sin
Databricks delante; el `readStream` que las consume es una envoltura fina.
"""

from __future__ import annotations

from typing import Any

RESCUED_COLUMN = "_rescued_data"


def options(*, entity: str, schema_location: str) -> dict[str, str]:
    """Opciones de lectura de Auto Loader para una entidad.

    El esquema se guarda en una ruta **por entidad**: compartirla mezclaría el
    estado de `orders` con el de `customers` y Auto Loader daría por ingeridos
    ficheros que no lo están.
    """
    return {
        "cloudFiles.format": "parquet",
        "cloudFiles.schemaLocation": f"{schema_location.rstrip('/')}/{entity}",
        # Bronze preserva lo que llega: una columna nueva en el origen se añade
        # a la tabla en lugar de hacer fallar la ingesta.
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        # Todo lo que no encaje en el esquema acaba aquí en vez de perderse.
        # Sin esta columna, un valor inesperado desaparecería en silencio, que
        # es exactamente lo contrario de lo que Bronze promete.
        "cloudFiles.rescuedDataColumn": RESCUED_COLUMN,
        # Los ficheros de aterrizaje ya son todo texto y el tipado es trabajo de
        # Silver. Inferir tipos aquí adelantaría la decisión a la capa
        # equivocada y haría fallar la ingesta ante el primer valor inválido,
        # que es precisamente uno de los casos que el pipeline debe manejar.
        "cloudFiles.inferColumnTypes": "false",
    }


def read_stream(spark: Any, *, source_path: str, entity: str, schema_location: str) -> Any:
    """Lector de Auto Loader sobre el directorio de una entidad.

    Solo funciona en Databricks: `cloudFiles` no existe en Spark de código
    abierto. Se valida ejecutándolo en el workspace, no con tests locales.
    """
    lector = spark.readStream.format("cloudFiles")
    for clave, valor in options(entity=entity, schema_location=schema_location).items():
        lector = lector.option(clave, valor)
    return lector.load(source_path)
