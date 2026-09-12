"""`gold.category_performance`: evolución mensual por categoría.

Grano: categoría x mes. Responde a qué partes del catálogo crecen y cuáles se
apagan, que es la pregunta que `product_performance` no puede contestar porque
no tiene eje temporal.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecommerce.analytics.daily_sales import ESTADOS_CON_INGRESO
from ecommerce.transformations import scd2


def build(order_items: DataFrame, orders: DataFrame, products: DataFrame) -> DataFrame:
    """Agrega las líneas de pedido a rendimiento por categoría y mes."""
    vendidas = (
        order_items.alias("i")
        .join(
            orders.filter(F.col("status").isin(*ESTADOS_CON_INGRESO))
            .filter(F.col("order_date").isNotNull())
            .select("order_id", "order_date")
            .alias("o"),
            on="order_id",
            how="inner",
        )
        .select("i.*", "o.order_date")
    )

    # La categoría se toma de la versión del producto vigente en la venta: si un
    # artículo se recategoriza, su historia de ventas no debe migrar de golpe a
    # la categoría nueva.
    con_producto = scd2.join_as_of(
        vendidas, products, key="product_id", timestamp_column="order_date"
    )

    return (
        con_producto
        # `trunc` devuelve el primer día del mes como fecha, no como texto. Un
        # mes en texto ordena mal al cambiar de año y no admite filtros por
        # rango en el dashboard.
        .withColumn("month", F.trunc("order_date", "month"))
        .groupBy("category", "month")
        .agg(
            F.count_distinct("order_id").alias("orders"),
            F.sum("quantity").alias("units_sold"),
            F.sum("line_total").alias("revenue"),
            F.sum(F.col("quantity") * F.col("cost")).alias("cost"),
        )
        .withColumn("profit", F.col("revenue") - F.col("cost"))
        .orderBy("month", F.col("revenue").desc())
    )
