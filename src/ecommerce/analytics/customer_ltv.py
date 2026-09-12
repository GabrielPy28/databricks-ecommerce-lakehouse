"""`gold.customer_lifetime_value`: valor acumulado por cliente.

Grano: un cliente. Alimenta la segmentación RFM y el análisis de cohortes.

**Sobre el nombre.** `lifetime_value` aquí es el **gasto histórico acumulado**,
no una predicción. Un CLV predictivo necesita un modelo de supervivencia y de
frecuencia de compra; llamar "valor de vida" a un modelo que no existe produce
cifras que nadie puede defender en una reunión. La columna se calcula y se
documenta como lo que es.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecommerce.analytics.daily_sales import ESCALA_TICKET_MEDIO, ESTADOS_CON_INGRESO
from ecommerce.transformations import scd2


def build(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """Agrega las órdenes a métricas acumuladas por cliente.

    El país se toma de la versión **vigente** del cliente, no de la de cada
    compra: la pregunta que responde esta tabla es "quiénes son mis clientes
    hoy y cuánto valen", y para eso interesa dónde están ahora. El desglose
    histórico por país lo da `category_performance` y los agregados diarios.
    """
    validas = orders.filter(F.col("status").isin(*ESTADOS_CON_INGRESO)).filter(
        F.col("order_date").isNotNull()
    )

    por_cliente = validas.groupBy("customer_id").agg(
        F.count("*").alias("orders"),
        F.sum("total_amount").alias("total_spend"),
        F.min(F.to_date("order_date")).alias("first_order"),
        F.max(F.to_date("order_date")).alias("last_order"),
    )

    vigentes = scd2.current(customers).select(
        "customer_id", *[c for c in customers.columns if c == "country"]
    )

    return (
        por_cliente.join(vigentes, on="customer_id", how="left")
        .withColumn(
            "average_order_value",
            (F.col("total_spend") / F.col("orders")).cast(ESCALA_TICKET_MEDIO),
        )
        # Días entre la primera y la última compra. Con una sola compra es 0, no
        # nulo: el cliente existe y su vida como comprador tiene longitud cero.
        .withColumn("active_days", F.datediff("last_order", "first_order"))
        # Gasto histórico, explícitamente. Ver la nota del módulo.
        .withColumn("lifetime_value", F.col("total_spend"))
        .orderBy(F.col("lifetime_value").desc())
    )
