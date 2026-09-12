"""Traducción del contrato de datos a tipos de Spark.

El contrato se declara una sola vez, en pyarrow, y se traduce aquí. La
alternativa —declararlo dos veces, una para el generador y otra para Spark— es
garantía de que ambas versiones divergen con el tiempo y de que la divergencia
se descubre tarde.

Este módulo importa PySpark, así que se mantiene separado del resto de
`schemas/`: el generador debe poder funcionar sin Spark instalado.
"""

from __future__ import annotations

import pyarrow as pa
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecommerce.schemas import ALL_SCHEMAS


def arrow_to_spark_type(tipo: pa.DataType) -> str:
    """Tipo de Arrow -> tipo SQL de Spark, como texto.

    Se devuelve texto y no un objeto porque es lo que consumen `try_cast` y
    `CREATE TABLE`, que es donde acaban usándose.
    """
    if pa.types.is_string(tipo):
        return "STRING"
    if pa.types.is_timestamp(tipo):
        return "TIMESTAMP"
    if pa.types.is_date(tipo):
        return "DATE"
    if pa.types.is_decimal(tipo):
        return f"DECIMAL({tipo.precision},{tipo.scale})"
    if pa.types.is_boolean(tipo):
        return "BOOLEAN"
    if pa.types.is_int64(tipo):
        return "BIGINT"
    if pa.types.is_integer(tipo):
        return "INT"
    raise ValueError(f"Sin traducción a Spark para el tipo Arrow {tipo}")


def arrow_to_spark_datatype(tipo: pa.DataType) -> DataType:
    """Tipo de Arrow -> objeto de tipo de Spark.

    Se construyen objetos en lugar de usar `StructType.fromDDL`, que necesita
    una sesión de Spark viva porque delega en el analizador sintáctico de la
    JVM. Traducir el contrato es una operación pura y no debería exigir un
    clúster arrancado.
    """
    if pa.types.is_string(tipo):
        return StringType()
    if pa.types.is_timestamp(tipo):
        return TimestampType()
    if pa.types.is_date(tipo):
        return DateType()
    if pa.types.is_decimal(tipo):
        return DecimalType(tipo.precision, tipo.scale)
    if pa.types.is_boolean(tipo):
        return BooleanType()
    if pa.types.is_int64(tipo):
        return LongType()
    if pa.types.is_integer(tipo):
        return IntegerType()
    raise ValueError(f"Sin traducción a Spark para el tipo Arrow {tipo}")


def spark_types(entity: str) -> dict[str, str]:
    """Nombre de columna -> tipo SQL de Spark, para una entidad del contrato."""
    return {campo.name: arrow_to_spark_type(campo.type) for campo in ALL_SCHEMAS[entity]}


def to_spark_schema(entity: str) -> StructType:
    return StructType(
        [
            # Todo admite nulos: el tipado ocurre sobre datos crudos que pueden
            # traer cualquier cosa. Rechazar es trabajo de la cuarentena, no
            # del esquema.
            StructField(campo.name, arrow_to_spark_datatype(campo.type), nullable=True)
            for campo in ALL_SCHEMAS[entity]
        ]
    )
