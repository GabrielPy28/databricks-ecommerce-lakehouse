"""`gold.customer_segments`: segmentación RFM.

Grano: un cliente. Clasifica la base de clientes en grupos accionables a partir
de tres señales:

* **R**ecencia: cuántos días desde la última compra.
* **F**recuencia: cuántas compras.
* **M**onetario: cuánto gasto acumulado.

Cada señal se convierte en un quintil (1 a 5) y la combinación determina el
segmento. Los quintiles son relativos a la propia base de clientes, lo que
evita tener que fijar umbrales absolutos que envejecen mal.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

QUINTILES = 5

# Reglas de asignación, en orden: la primera que se cumple gana. El orden
# importa —un cliente puede encajar en varias— y se recorre de la condición más
# específica a la más general.
#
# Los nombres son los habituales del marco RFM, en el idioma en que se usan en
# la práctica, para que un analista los reconozca sin traducción.
SEGMENTOS = (
    ("Champions", "r_score >= 4 AND f_score >= 4 AND m_score >= 4"),
    ("Loyal", "f_score >= 4 AND r_score >= 3"),
    ("Big Spenders", "m_score >= 4 AND r_score >= 3"),
    ("Promising", "r_score >= 4 AND f_score <= 2"),
    ("At Risk", "r_score <= 2 AND (f_score >= 3 OR m_score >= 3)"),
    ("Hibernating", "r_score <= 2 AND f_score <= 2 AND m_score <= 2"),
    ("Needs Attention", "r_score = 3"),
)
SEGMENTO_POR_DEFECTO = "Others"


def build(ltv: DataFrame, *, reference_date: date) -> DataFrame:
    """Segmenta la base de clientes a partir de `gold.customer_lifetime_value`.

    `reference_date` se recibe y no se toma de `current_date()`: con la fecha
    del sistema, la segmentación cambiaría cada día y dejaría de ser
    reproducible, así que dos ejecuciones del mismo pipeline sobre los mismos
    datos darían resultados distintos.

    En el pipeline se le pasa la fecha máxima de orden presente en los datos,
    que es la referencia correcta para un histórico cerrado.
    """
    con_recencia = ltv.withColumn(
        "recency_days", F.datediff(F.lit(reference_date), F.col("last_order"))
    )

    # `ntile` reparte en quintiles sobre el conjunto ordenado.
    #
    # La recencia se ordena **descendente** a propósito: menos días es mejor, y
    # ordenarla como las otras dos produciría una segmentación que llama
    # campeones a los clientes que llevan un año sin aparecer.
    ventana_r = Window.orderBy(F.col("recency_days").desc())
    ventana_f = Window.orderBy(F.col("orders").asc())
    ventana_m = Window.orderBy(F.col("total_spend").asc())

    puntuado = (
        con_recencia.withColumn("r_score", F.ntile(QUINTILES).over(ventana_r))
        .withColumn("f_score", F.ntile(QUINTILES).over(ventana_f))
        .withColumn("m_score", F.ntile(QUINTILES).over(ventana_m))
    )

    segmento = F.lit(SEGMENTO_POR_DEFECTO)
    for nombre, condicion in reversed(SEGMENTOS):
        segmento = F.when(F.expr(condicion), F.lit(nombre)).otherwise(segmento)

    return puntuado.withColumn("segment", segmento).orderBy(
        F.col("m_score").desc(), F.col("r_score").desc()
    )
