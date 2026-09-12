"""Carga idempotente en Silver mediante MERGE.

Es la pieza de la que dependen dos promesas del proyecto:

* **Idempotencia.** Reprocesar un lote ya cargado no cambia nada.
* **Datos tardíos.** Un registro que pertenece al pasado pero llega ahora no
  revierte el estado actual.

Ambas salen de la misma condición: solo se actualiza cuando la versión de
origen es estrictamente más reciente que la almacenada.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def deduplicate_latest(df: DataFrame, *, key: str | list[str], order_by: str) -> DataFrame:
    """Deja una sola fila por clave: la de `order_by` más alto.

    Delta rechaza un MERGE cuyo origen traiga varias filas para la misma clave,
    y un lote real siempre las trae: el sistema de origen reemite registros y
    una orden puede haber mutado dos veces dentro de la misma ventana.

    `key` admite varias columnas. Lo necesitan las dimensiones SCD2: agrupar
    solo por la clave de negocio se quedaría con la versión más reciente y
    destruiría el historial, mientras que agrupar por clave y marca de tiempo
    conserva una fila por versión.
    """
    columnas = [key] if isinstance(key, str) else key
    ventana = Window.partitionBy(*columnas).orderBy(F.col(order_by).desc())
    return df.withColumn("_rn", F.row_number().over(ventana)).filter(F.col("_rn") == 1).drop("_rn")


def merge_latest(target: DeltaTable, source: DataFrame, *, key: str, order_by: str) -> None:
    """Mezcla `source` en `target` conservando siempre la versión más reciente.

    La condición `s.<order_by> > t.<order_by>` hace tres cosas a la vez:

    * reejecutar el mismo lote no actualiza nada (los valores son iguales, no
      mayores), así que la operación es idempotente;
    * un registro tardío se ignora en lugar de sobrescribir el estado bueno;
    * una versión nueva sí se aplica.

    Sin ella, `whenMatchedUpdateAll()` a secas devolvería una orden entregada
    al estado `pending` en cuanto llegara un mensaje retrasado, y el dato
    quedaría corrupto sin que nada fallara.
    """
    unicos = deduplicate_latest(source, key=key, order_by=order_by)
    (
        target.alias("t")
        .merge(unicos.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll(condition=f"s.{order_by} > t.{order_by}")
        .whenNotMatchedInsertAll()
        .execute()
    )
