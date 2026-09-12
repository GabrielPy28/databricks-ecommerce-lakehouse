"""Composición de la capa Silver: de Bronze a tablas validadas.

La cuarentena es lo que separa este proyecto de un ETL de ejemplo. Una fila
corrupta no se descarta —perderla en silencio es peor que no procesarla— sino
que se aparta junto con el motivo, queda contabilizada y es reprocesable
cuando se corrige la causa.

Una fila puede fallar por dos vías distintas:

* **Conversión** (`_cast_errors`): el valor no es del tipo que dice el
  contrato. Una fecha que no es una fecha.
* **Reglas de negocio** (`_rule_errors`): el valor es del tipo correcto pero
  no tiene sentido. Un identificador de cliente que no existe.

Ambas acaban en la misma columna `_errors`. Mantener dos mecanismos separados
obligaría a mirar en dos sitios para saber por qué se rechazó una fila, y a
reconciliar dos recuentos que deberían ser uno.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecommerce.quality.engine import RULE_ERRORS
from ecommerce.transformations.cleaning import CAST_ERRORS

ERRORS = "_errors"

# Columnas de error que se funden en `_errors`, si están presentes.
_ORIGENES = (CAST_ERRORS, RULE_ERRORS)


def combine_errors(df: DataFrame) -> DataFrame:
    """Funde las columnas de error en una sola, `_errors`.

    Tolera que falte alguna: Silver puede ejecutarse solo con validación de
    tipos, sin haber evaluado reglas todavía.
    """
    presentes = [columna for columna in _ORIGENES if columna in df.columns]
    if not presentes:
        return df.withColumn(ERRORS, F.array().cast("array<string>"))
    if ERRORS in df.columns:
        return df

    return df.withColumn(ERRORS, F.concat(*presentes)).drop(*presentes)


def split_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separa las filas válidas de las rechazadas.

    Devuelve `(validas, cuarentena)`. Ninguna fila se pierde: la suma de ambas
    es siempre el total de entrada.

    La cuarentena conserva `_errors`, de modo que el reporte de calidad puede
    decir qué falló exactamente en cada fila en lugar de limitarse a contar
    registros rechazados.
    """
    combinado = combine_errors(df)
    rechazada = F.size(F.col(ERRORS)) > 0
    return combinado.filter(~rechazada).drop(ERRORS), combinado.filter(rechazada)
