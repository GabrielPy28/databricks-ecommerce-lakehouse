"""`gold.product_performance`: rendimiento por producto.

Grano: un producto. Responde a qué se vende, cuánto margen deja y qué se
devuelve.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecommerce.analytics.daily_sales import ESTADOS_CON_INGRESO
from ecommerce.transformations import scd2

# Escala del porcentaje. Se usa `double` y no `decimal`: es una proporción para
# mostrar, no un importe que deba cuadrar al céntimo.
PORCENTAJE = "double"


def build(
    order_items: DataFrame,
    orders: DataFrame,
    products: DataFrame,
    returns: DataFrame,
) -> DataFrame:
    """Agrega las líneas de pedido a rendimiento por producto.

    `products` es la dimensión historificada completa, no solo sus versiones
    vigentes: el coste se toma del que estaba en vigor **en la fecha de la
    venta**. Usar el coste actual para todo el histórico inventa margen —o lo
    destruye— cada vez que cambia un precio de compra.
    """
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

    # Aquí rinde el historial: cada línea se valora con el coste de su momento.
    con_producto = scd2.join_as_of(
        vendidas, products, key="product_id", timestamp_column="order_date"
    )

    por_producto = con_producto.groupBy("product_id", "product_name", "category").agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("line_total").alias("revenue"),
        F.sum(F.col("quantity") * F.col("cost")).alias("cost"),
        F.count_distinct("order_id").alias("orders"),
    )

    devueltas = returns.groupBy("product_id").agg(
        F.sum("quantity").alias("returned_units"),
        F.sum("refund_amount").alias("refunded_amount"),
    )

    return (
        por_producto.join(devueltas, on="product_id", how="left")
        # Un producto sin devoluciones tiene cero, no nulo: en un dashboard, un
        # nulo se lee como "no se sabe", y aquí sí se sabe.
        .withColumn("returned_units", F.coalesce("returned_units", F.lit(0)))
        .withColumn(
            "refunded_amount", F.coalesce("refunded_amount", F.lit(0).cast("decimal(12,2)"))
        )
        .withColumn("profit", F.col("revenue") - F.col("cost"))
        .withColumn(
            "margin_pct",
            F.when(F.col("revenue") > 0, 100 * F.col("profit") / F.col("revenue"))
            .otherwise(F.lit(0))
            .cast(PORCENTAJE),
        )
        .withColumn(
            "return_rate",
            F.when(F.col("units_sold") > 0, 100 * F.col("returned_units") / F.col("units_sold"))
            .otherwise(F.lit(0))
            .cast(PORCENTAJE),
        )
        .orderBy(F.col("revenue").desc())
    )
