"""`gold.marketing_funnel`: conversión por día y canal.

Grano: día x canal de adquisición. Responde a qué canales traen tráfico que
compra y cuáles traen tráfico que se va.

**Se cuentan sesiones, no eventos.** Tres vistas de página de la misma visita
son una visita, no tres. Contando eventos, la tasa de conversión quedaría
dividida por el número de páginas que el visitante mirase, y un canal que trae
gente curiosa parecería peor que uno que trae gente que rebota.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

PORCENTAJE = "double"

# Pasos del embudo, del más amplio al más estrecho.
PASOS = (
    ("page_view", "page_views"),
    ("add_to_cart", "add_to_carts"),
    ("checkout_start", "checkouts"),
    ("purchase", "purchases"),
)


def _sesiones_con(evento: str) -> Column:
    """Sesiones distintas que alcanzaron un paso del embudo."""
    return F.count_distinct(F.when(F.col("event_type") == evento, F.col("session_id")))


def _tasa(numerador: str, denominador: str) -> Column:
    """Porcentaje, con cero —no nulo— cuando no hay base sobre la que dividir.

    Un nulo en un dashboard se lee como "no se sabe". Un canal que no convirtió
    ninguna visita sí se sabe: convirtió cero.
    """
    return (
        F.when(F.col(denominador) > 0, 100 * F.col(numerador) / F.col(denominador))
        .otherwise(F.lit(0))
        .cast(PORCENTAJE)
    )


def build(web_events: DataFrame) -> DataFrame:
    """Agrega los eventos de navegación a embudo diario por canal."""
    return (
        web_events.filter(F.col("event_timestamp").isNotNull())
        .withColumn("date", F.to_date("event_timestamp"))
        .withColumnRenamed("utm_source", "channel")
        .groupBy("date", "channel")
        .agg(
            F.count_distinct("session_id").alias("sessions"),
            *[_sesiones_con(evento).alias(alias) for evento, alias in PASOS],
        )
        # Cada tasa se mide contra el paso inmediatamente anterior, que es lo que
        # localiza dónde se pierde la gente. La conversión global, en cambio, va
        # sobre las visitas.
        .withColumn("cart_rate", _tasa("add_to_carts", "page_views"))
        .withColumn("checkout_rate", _tasa("checkouts", "add_to_carts"))
        .withColumn("payment_rate", _tasa("purchases", "checkouts"))
        .withColumn("conversion_rate", _tasa("purchases", "page_views"))
        .orderBy("date", F.col("page_views").desc())
    )
