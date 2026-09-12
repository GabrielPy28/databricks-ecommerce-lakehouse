"""Normalización y tipado: de texto crudo al contrato lógico.

Funciones puras `DataFrame -> DataFrame`. Los notebooks solo las encadenan.
Esa separación es lo que permite que estos tests corran en el contenedor en
segundos en lugar de necesitar un workspace.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ecommerce.schemas.spark import spark_types

CAST_ERRORS = "_cast_errors"


def trim_strings(df: DataFrame, columns: list[str]) -> DataFrame:
    for columna in columns:
        df = df.withColumn(columna, F.trim(F.col(columna)))
    return df


def normalize_status(df: DataFrame, column: str) -> DataFrame:
    """Unifica mayúsculas y espacios.

    El sistema de origen emite ` DELIVERED `, `delivered` y `Delivered ` para
    el mismo estado. Sin normalizar, la validación de dominio rechazaría
    valores legítimos mal escritos, y el desglose por estado en Gold contaría
    el mismo estado varias veces.
    """
    return df.withColumn(column, F.lower(F.trim(F.col(column))))


def _try_cast(columna: str, tipo: str) -> Column:
    """Conversión que devuelve nulo en lugar de lanzar excepción.

    Spark 4 activa el modo ANSI por defecto: un `cast` inválido aborta el job
    entero. Un único registro corrupto entre millones tumbaría el pipeline.
    `try_cast` lo convierte en nulo y deja que la cuarentena lo recoja, que es
    el comportamiento que se quiere en una capa de validación.
    """
    return F.expr(f"try_cast(`{columna}` AS {tipo})")


def cast_to_contract(df: DataFrame, entity: str) -> DataFrame:
    """Convierte las columnas de texto a los tipos del contrato lógico."""
    for columna, tipo in spark_types(entity).items():
        if columna in df.columns:
            df = df.withColumn(columna, _try_cast(columna, tipo))
    return df


def with_cast_errors(df: DataFrame, entity: str) -> DataFrame:
    """Convierte al contrato y añade `_cast_errors` con las columnas que fallaron.

    Un nulo después de convertir puede venir de un origen nulo o de un valor
    corrupto. Distinguirlos es lo que permite explicar la cuarentena en lugar
    de limitarse a decir que "había nulos".
    """
    tipos = {c: t for c, t in spark_types(entity).items() if c in df.columns}

    fallos = [
        F.when(F.col(columna).isNotNull() & _try_cast(columna, tipo).isNull(), F.lit(columna))
        for columna, tipo in tipos.items()
    ]

    errores = F.array_compact(F.array(*fallos)) if fallos else F.array().cast("array<string>")

    # El array se calcula ANTES de convertir. Hacerlo después no detectaría
    # nada: la columna inválida ya sería nula, y la comprobación "no era nula
    # pero al convertir sí lo es" resultaría siempre falsa.
    return cast_to_contract(df.withColumn(CAST_ERRORS, errores), entity)
