"""`gold.daily_sales`: la tabla de cabecera del dashboard.

Grano: un día. Responde a cuánto se vendió, a cuántos clientes y con qué
ticket medio.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Días hacia atrás que se recalculan además de los del lote.
#
# Cubre los datos tardíos: un registro con fecha de hace tres días que llega
# hoy cambia el total de aquel día, y sin margen ese día quedaría desfasado
# para siempre. El valor debe ser mayor que el retraso máximo que produce el
# generador.
LOOKBACK_DAYS = 7

# Estados que representan una venta reconocida.
#
# No toda orden es ingreso: una cancelada o una pendiente de pago no lo son, y
# contarlas inflaría los ingresos del dashboard sin que nada fallara. Se
# excluye también `returned`, cuyo tratamiento corresponde a la tabla de
# devoluciones.
ESTADOS_CON_INGRESO = ("paid", "shipped", "delivered")

# Escala del ticket medio. Una división entre enteros truncaría a la unidad y
# convertiría un ticket de 89,90 en 89.
ESCALA_TICKET_MEDIO = "decimal(12,2)"


def build(orders: DataFrame) -> DataFrame:
    """Agrega las órdenes de Silver a ventas diarias."""
    return (
        orders
        # Una fecha nula significa que la orden no pertenece a ningún día.
        # Agruparla generaría una fila con fecha nula en el dashboard, que es
        # ruido sin significado para quien lo mira.
        .filter(F.col("order_date").isNotNull())
        .filter(F.col("status").isin(*ESTADOS_CON_INGRESO))
        .withColumn("date", F.to_date(F.col("order_date")))
        .groupBy("date")
        .agg(
            F.count("*").alias("orders"),
            # Distintos, no total: un cliente que compra tres veces en un día
            # es un cliente, no tres.
            F.count_distinct("customer_id").alias("customers"),
            F.sum("gross_amount").alias("gross_revenue"),
            F.sum("discount_amount").alias("discounts"),
            F.sum("total_amount").alias("net_revenue"),
        )
        .withColumn(
            "average_order_value",
            (F.col("net_revenue") / F.col("orders")).cast(ESCALA_TICKET_MEDIO),
        )
        .orderBy("date")
    )


def affected_range(batch_orders: DataFrame, *, lookback_days: int = LOOKBACK_DAYS):
    """Rango de días que un lote obliga a recalcular.

    Va desde el día más antiguo del lote menos el margen, hasta el más
    reciente. Devuelve `None` si el lote no aporta ninguna fecha: no tener nada
    que recalcular es distinto de recalcularlo todo, y confundirlos llevaría a
    reescribir la tabla entera con datos parciales.
    """
    limites = (
        batch_orders.filter(F.col("order_date").isNotNull())
        .select(
            F.min(F.to_date("order_date")).alias("desde"),
            F.max(F.to_date("order_date")).alias("hasta"),
        )
        .first()
    )
    if limites is None or limites.desde is None:
        return None

    return limites.desde - timedelta(days=lookback_days), limites.hasta


def rebuild_window(all_orders: DataFrame, *, desde: date, hasta: date) -> DataFrame:
    """Reconstruye `daily_sales` solo para un rango de días.

    Recibe **todas** las órdenes, no las del lote. Es la parte que no se puede
    atajar: para recalcular un día hay que sumar todas sus órdenes, no solo las
    que acaban de llegar. Reconstruirlo con el lote reescribiría el día con un
    total parcial y los ingresos caerían sin que nada fallara.
    """
    del_rango = all_orders.filter(
        F.col("order_date").isNotNull()
        & (F.to_date("order_date") >= F.lit(desde))
        & (F.to_date("order_date") <= F.lit(hasta))
    )
    return build(del_rango)


def replace_where(desde: date, hasta: date) -> str:
    """Predicado para la escritura selectiva de Delta.

    Con `replaceWhere`, Delta sustituye solo las particiones que cumplen el
    predicado y deja intacto el resto de la tabla.
    """
    return f"date >= '{desde.isoformat()}' AND date <= '{hasta.isoformat()}'"
