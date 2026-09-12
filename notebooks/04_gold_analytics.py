# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — productos de datos
# MAGIC
# MAGIC Aquí los datos dejan de ser tablas y pasan a responder preguntas de
# MAGIC negocio.
# MAGIC
# MAGIC ## Dos estrategias de recarga, por motivos distintos
# MAGIC
# MAGIC **`daily_sales` se recarga por ventana.** Su grano es el día, así que un
# MAGIC lote solo invalida unos pocos días. Se recalculan esos, más un margen
# MAGIC hacia atrás que cubre los **datos tardíos**: un registro con fecha de hace
# MAGIC tres días que llega hoy cambia el total de aquel día.
# MAGIC
# MAGIC Dos detalles que hacen que eso sea correcto y no solo rápido:
# MAGIC
# MAGIC * El día se reconstruye con **todas** sus órdenes, no solo con las del
# MAGIC   lote. Reconstruirlo con el lote lo reescribiría con un total parcial y
# MAGIC   los ingresos caerían sin que nada fallara.
# MAGIC * Se usa `replaceWhere`, que sustituye únicamente las filas del rango y
# MAGIC   deja intacto el resto de la tabla. Funciona sobre columnas de datos,
# MAGIC   así que no hace falta particionar por fecha —y particionar por fecha
# MAGIC   una tabla de agregados diarios resultó ser un error medido: una
# MAGIC   partición por fila.
# MAGIC
# MAGIC **El resto se reconstruye completo.** Su grano es el cliente o el
# MAGIC producto, no el día: una sola orden nueva puede cambiar el segmento de un
# MAGIC cliente y, al ser quintiles relativos, también el de otros. No hay una
# MAGIC partición que aislar, y a esta escala la reconstrucción cuesta segundos.
# MAGIC
# MAGIC Sin `run_id`, también `daily_sales` se reconstruye entera.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Ejecución (vacío = reconstrucción completa)")

# COMMAND ----------

from pyspark.sql import functions as F

from ecommerce import config
from ecommerce.analytics import (
    category_performance,
    customer_ltv,
    customer_segments,
    daily_sales,
    marketing_funnel,
    product_performance,
)
from ecommerce.schemas import metadata
from ecommerce.transformations import cleaning

ns = config.Namespace()
run_id = dbutils.widgets.get("run_id").strip()


def silver(nombre: str):
    return spark.table(ns.table("silver", nombre))


def publicar(df, nombre: str, comentarios: dict[str, str], cluster_by: str | None = None) -> None:
    """Escribe una tabla Gold, la organiza y documenta sus columnas.

    Gold es la capa que consume negocio: debe explicarse sola en el catálogo,
    sin tener que preguntar a quien la construyó.

    **Sobre `cluster_by` en lugar de `partitionBy`.** La primera versión
    particionaba estas tablas por su columna de fecha, y medirlo dejó claro que
    era un error: `daily_sales` acabó con 698 ficheros de 2,2 KB —una fila por
    fichero— y `marketing_funnel` con 721. Una partición por día sobre una tabla
    de agregados diarios crea una partición por fila.

    `OPTIMIZE` tampoco lo arreglaba: compacta *dentro* de cada partición, y con
    un solo fichero por partición no tiene nada que compactar. Por eso ni
    siquiera dejaba entrada en el historial de la tabla.

    El liquid clustering agrupa por la misma columna sin trocear el
    almacenamiento, así que el salto de datos sigue funcionando y los ficheros
    tienen un tamaño razonable.
    """
    destino = ns.table("gold", nombre)

    # Se recrea la tabla: una tabla ya particionada no se convierte a clustering
    # sobre la marcha, y estas se reconstruyen enteras de todas formas.
    if cluster_by:
        spark.sql(f"DROP TABLE IF EXISTS {destino}")

    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)

    if cluster_by:
        spark.sql(f"ALTER TABLE {destino} CLUSTER BY ({cluster_by})")
        # Con liquid clustering es `OPTIMIZE` quien agrupa los datos de verdad;
        # sin él, el clustering queda declarado pero no aplicado.
        spark.sql(f"OPTIMIZE {destino}")

    for columna, comentario in comentarios.items():
        spark.sql(f"ALTER TABLE {destino} ALTER COLUMN {columna} COMMENT '{comentario}'")

    detalle = spark.sql(f"DESCRIBE DETAIL {destino}").first()
    medio_kb = detalle.sizeInBytes / detalle.numFiles / 1024 if detalle.numFiles else 0
    print(
        f"{destino:<42} {spark.table(destino).count():>10,} filas"
        f"  {detalle.numFiles:>4} fichero(s) de {medio_kb:>7.1f} KB"
    )


# COMMAND ----------

# MAGIC %md ## daily_sales — recarga por ventana

# COMMAND ----------

ordenes = silver("orders")
destino_ventas = ns.table("gold", "daily_sales")

ventana = None
if run_id:
    # El rango se deriva de lo que **esta ejecución** ingirió, no de una
    # etiqueta de lote.
    #
    # Auto Loader procesa todos los ficheros nuevos que encuentre, que pueden
    # ser de varios lotes a la vez. Filtrar por `_batch_id` daría una ventana
    # equivocada en cuanto una ejecución arrastre más de un lote —y no fallaría:
    # dejaría días sin recalcular en silencio. `_pipeline_run_id` identifica
    # exactamente lo que entró en esta corrida.
    del_run = spark.table(ns.table("bronze", "orders_raw")).filter(
        f"{metadata.PIPELINE_RUN_ID} = '{run_id}'"
    )
    ventana = daily_sales.affected_range(cleaning.cast_to_contract(del_run, "orders"))
    if ventana is None:
        print(
            f"La ejecución {run_id} no ingirió órdenes con fecha utilizable. "
            f"Se reconstruye la tabla completa."
        )

COMENTARIOS_VENTAS = {
    "date": "Día natural de la orden",
    "orders": "Órdenes con ingreso reconocido (paid, shipped, delivered)",
    "customers": "Clientes distintos que compraron ese día",
    "gross_revenue": "Ingreso antes de descuentos",
    "discounts": "Descuentos aplicados",
    "net_revenue": "Ingreso después de descuentos",
    "average_order_value": "Ticket medio: net_revenue / orders",
}

if ventana is None:
    publicar(daily_sales.build(ordenes), "daily_sales", COMENTARIOS_VENTAS, cluster_by="date")
else:
    desde, hasta = ventana
    resultado = daily_sales.rebuild_window(ordenes, desde=desde, hasta=hasta)
    (
        resultado.write.mode("overwrite")
        .option("replaceWhere", daily_sales.replace_where(desde, hasta))
        .saveAsTable(destino_ventas)
    )
    print(
        f"{destino_ventas}: recalculados {resultado.count():,} días "
        f"en la ventana [{desde} .. {hasta}] (ejecución {run_id})"
    )

# COMMAND ----------

# MAGIC %md ## Producto y categoría
# MAGIC
# MAGIC Ambas valoran cada línea con el **coste vigente en la fecha de la venta**,
# MAGIC tomado del historial SCD2 de `products`. Usar el coste actual para todo el
# MAGIC histórico inventaría margen cada vez que cambia un precio de compra.

# COMMAND ----------

lineas = silver("order_items")
productos = silver("products")

publicar(
    product_performance.build(lineas, ordenes, productos, silver("returns")),
    "product_performance",
    {
        "product_id": "Identificador del producto",
        "units_sold": "Unidades vendidas en órdenes con ingreso reconocido",
        "revenue": "Ingreso neto de las líneas vendidas",
        "cost": "Coste de lo vendido, al coste vigente en cada fecha de venta",
        "profit": "revenue - cost",
        "margin_pct": "Margen porcentual sobre el ingreso",
        "returned_units": "Unidades devueltas",
        "return_rate": "Porcentaje de unidades devueltas sobre vendidas",
    },
)

publicar(
    category_performance.build(lineas, ordenes, productos),
    "category_performance",
    {
        "category": "Categoría del producto en la fecha de la venta",
        "month": "Primer día del mes",
        "orders": "Órdenes distintas con al menos una línea de la categoría",
        "revenue": "Ingreso neto del mes",
        "profit": "revenue - cost",
    },
    cluster_by="month",
)

# COMMAND ----------

# MAGIC %md ## Cliente: valor acumulado y segmentación

# COMMAND ----------

clientes = silver("customers")
ltv = customer_ltv.build(ordenes, clientes)

publicar(
    ltv,
    "customer_lifetime_value",
    {
        "customer_id": "Identificador del cliente",
        "country": "País en la versión vigente del cliente",
        "orders": "Órdenes con ingreso reconocido",
        "total_spend": "Gasto acumulado",
        "average_order_value": "Ticket medio del cliente",
        "first_order": "Fecha de la primera compra",
        "last_order": "Fecha de la última compra",
        "active_days": "Días entre la primera y la última compra",
        "lifetime_value": "Gasto historico acumulado, NO una prediccion",
    },
)

# COMMAND ----------

# La fecha de referencia de la recencia sale del propio dato, no del reloj: con
# `current_date()`, dos ejecuciones del mismo pipeline sobre los mismos datos
# darían segmentos distintos.
referencia = (
    spark.table(ns.table("gold", "customer_lifetime_value"))
    .agg(F.max("last_order").alias("r"))
    .first()
    .r
)
print(f"Fecha de referencia para la recencia: {referencia}")

publicar(
    customer_segments.build(
        spark.table(ns.table("gold", "customer_lifetime_value")), reference_date=referencia
    ),
    "customer_segments",
    {
        "recency_days": "Días desde la última compra hasta la fecha de referencia",
        "r_score": "Quintil de recencia (5 = compró más recientemente)",
        "f_score": "Quintil de frecuencia (5 = más órdenes)",
        "m_score": "Quintil monetario (5 = más gasto)",
        "segment": "Segmento RFM",
    },
)

# COMMAND ----------

# MAGIC %md ## Embudo de marketing
# MAGIC
# MAGIC Cuenta **sesiones, no eventos**: tres vistas de página de la misma visita
# MAGIC son una visita. Contando eventos, la conversión quedaría dividida por el
# MAGIC número de páginas que el visitante mirase.

# COMMAND ----------

publicar(
    marketing_funnel.build(silver("web_events")),
    "marketing_funnel",
    {
        "date": "Día del evento",
        "channel": "Canal de adquisición (utm_source)",
        "sessions": "Sesiones distintas",
        "page_views": "Sesiones que vieron al menos una página",
        "add_to_carts": "Sesiones que añadieron al carrito",
        "checkouts": "Sesiones que iniciaron el pago",
        "purchases": "Sesiones que compraron",
        "conversion_rate": "Porcentaje de visitas que compraron",
    },
    cluster_by="date",
)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {ns.catalog}.gold"))
