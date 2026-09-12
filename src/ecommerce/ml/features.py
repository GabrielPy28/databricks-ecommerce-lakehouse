"""Variables y etiqueta para el modelo de fuga.

**Todo se calcula respecto a un corte.** Las variables solo miran datos con
fecha menor o igual al corte; la etiqueta, estrictamente posteriores. Esa
separación es lo único que impide que el modelo aprenda la respuesta en vez de
predecirla.

El horizonte por defecto son 90 días: suficiente para que un comprador habitual
haya vuelto, y corto como para que la predicción sirva para actuar.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecommerce.analytics.daily_sales import ESTADOS_CON_INGRESO

HORIZONTE_DIAS = 90

# Variables que entran al modelo. El orden es el del vector de características.
FEATURE_COLUMNS = (
    "recency_days",
    "frequency",
    "monetary",
    "avg_order_value",
    "tenure_days",
    "orders_last_30d",
    "orders_last_90d",
    "spend_last_90d",
    "avg_days_between_orders",
    "return_rate",
)

LABEL_COLUMN = "churned"


def _con_ingreso(orders: DataFrame) -> DataFrame:
    """Órdenes que cuentan como actividad comercial.

    Una cancelada no lo es: contarla haría parecer activo a un cliente que
    intentó comprar y se echó atrás, que es justo la señal contraria.
    """
    return orders.filter(F.col("status").isin(*ESTADOS_CON_INGRESO)).filter(
        F.col("order_date").isNotNull()
    )


def build_features(orders: DataFrame, returns: DataFrame, *, cutoff: date) -> DataFrame:
    """Variables por cliente calculadas **solo** con datos hasta el corte.

    Añadir actividad posterior al corte no debe cambiar ni una cifra de esta
    tabla. Hay un test que lo comprueba, porque es la propiedad que separa un
    modelo de una tautología.

    Solo aparecen los clientes que ya habían comprado antes del corte: no se
    puede predecir la fuga de quien todavía no era cliente.
    """
    limite = F.lit(cutoff)
    historicas = _con_ingreso(orders).filter(F.to_date("order_date") <= limite)

    def en_ventana(dias: int, expresion) -> F.Column:
        desde = F.date_sub(limite, dias)
        return F.sum(F.when(F.to_date("order_date") > desde, expresion).otherwise(F.lit(0)))

    agregadas = historicas.groupBy("customer_id").agg(
        F.count("*").alias("frequency"),
        F.sum("total_amount").alias("monetary"),
        F.min(F.to_date("order_date")).alias("_primera"),
        F.max(F.to_date("order_date")).alias("_ultima"),
        en_ventana(30, F.lit(1)).alias("orders_last_30d"),
        en_ventana(90, F.lit(1)).alias("orders_last_90d"),
        en_ventana(90, F.col("total_amount")).alias("spend_last_90d"),
    )

    # Devoluciones: solo las de órdenes anteriores al corte. Una devolución de
    # una compra posterior sería información del futuro.
    devoluciones = (
        returns.join(historicas.select("order_id", "customer_id"), on="order_id", how="inner")
        .groupBy("customer_id")
        .agg(F.count_distinct("order_id").alias("_ordenes_devueltas"))
    )

    return (
        agregadas.join(devoluciones, on="customer_id", how="left")
        .withColumn("recency_days", F.datediff(limite, F.col("_ultima")))
        .withColumn("tenure_days", F.datediff(limite, F.col("_primera")))
        .withColumn(
            "avg_order_value",
            (F.col("monetary") / F.col("frequency")).cast("decimal(12,2)"),
        )
        .withColumn(
            "avg_days_between_orders",
            # Con una sola compra no hay intervalo que medir. Poner cero diría
            # "compra sin parar", lo contrario de la verdad; la antigüedad
            # refleja que lleva todo ese tiempo sin repetir.
            F.when(
                F.col("frequency") > 1,
                F.datediff(F.col("_ultima"), F.col("_primera")) / (F.col("frequency") - 1),
            )
            .otherwise(F.col("tenure_days"))
            .cast("double"),
        )
        .withColumn(
            "return_rate",
            (100.0 * F.coalesce("_ordenes_devueltas", F.lit(0)) / F.col("frequency")).cast(
                "double"
            ),
        )
        .select("customer_id", *FEATURE_COLUMNS)
    )


def build_label(
    orders: DataFrame, *, cutoff: date, horizon_days: int = HORIZONTE_DIAS
) -> DataFrame:
    """1 si el cliente **no** volvió a comprar en la ventana posterior al corte.

    La ventana es cerrada por la derecha: una compra después del horizonte no
    cuenta como retención. Si contara, la etiqueta dependería de cuántos datos
    haya disponibles después y no del comportamiento del cliente.
    """
    limite = F.lit(cutoff)
    fin = F.date_add(limite, horizon_days)

    activos = (
        _con_ingreso(orders)
        .filter(F.to_date("order_date") <= limite)
        .select("customer_id")
        .distinct()
    )
    volvieron = (
        _con_ingreso(orders)
        .filter((F.to_date("order_date") > limite) & (F.to_date("order_date") <= fin))
        .select("customer_id")
        .distinct()
        .withColumn("_volvio", F.lit(1))
    )

    return (
        activos.join(volvieron, on="customer_id", how="left")
        # Ausente en la ventana significa fuga, no dato desconocido. Dejarlo
        # nulo descartaría justo los casos positivos al entrenar.
        .withColumn(LABEL_COLUMN, F.when(F.col("_volvio").isNull(), 1).otherwise(0))
        .select("customer_id", LABEL_COLUMN)
    )


def build_training_set(
    orders: DataFrame,
    returns: DataFrame,
    *,
    cutoff: date,
    horizon_days: int = HORIZONTE_DIAS,
) -> DataFrame:
    """Variables más etiqueta para un corte: una fila por cliente activo."""
    variables = build_features(orders, returns, cutoff=cutoff)
    etiquetas = build_label(orders, cutoff=cutoff, horizon_days=horizon_days)
    return variables.join(etiquetas, on="customer_id", how="inner")
