# Databricks notebook source
# MAGIC %md
# MAGIC # Predicción de fuga de clientes
# MAGIC
# MAGIC Fase opcional. El eje del proyecto sigue siendo la ingeniería de datos:
# MAGIC este cuaderno **consume** Gold y no toca el pipeline.
# MAGIC
# MAGIC ## Lo que hace difícil un modelo de fuga
# MAGIC
# MAGIC No es predecir mal: es predecir **demasiado bien**. Si la fuga se define
# MAGIC como "sin compras en 90 días" y entre las variables va
# MAGIC `days_since_last_order` calculado sobre todo el histórico, el modelo saca
# MAGIC un AUC de 0,99 y no sirve para nada: le hemos dado la respuesta.
# MAGIC
# MAGIC Aquí hay dos cortes y todo se cuelga de ellos:
# MAGIC
# MAGIC ```
# MAGIC   variables (≤ T1)   etiqueta (T1, T1+90]        ← entrenamiento
# MAGIC                      variables (≤ T2)   etiqueta (T2, T2+90]   ← prueba
# MAGIC ```
# MAGIC
# MAGIC La evaluación es **fuera de tiempo**: el modelo se juzga sobre un periodo
# MAGIC posterior al que aprendió. Un reparto aleatorio 80/20 del mismo periodo
# MAGIC dejaría al modelo ver el futuro de sus propios clientes y daría una cifra
# MAGIC bonita e irreal.
# MAGIC
# MAGIC Y toda métrica va acompañada de la **regla trivial** —cuanto más tiempo
# MAGIC sin comprar, más riesgo—, que es lo que el negocio ya sabe hacer sin
# MAGIC modelo. Si el modelo no la supera, no hay nada que desplegar.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from datetime import timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

from ecommerce import config
from ecommerce.ml import churn, features

ns = config.Namespace()
HORIZONTE = features.HORIZONTE_DIAS

ordenes = spark.table(ns.table("silver", "orders"))
devoluciones = spark.table(ns.table("silver", "returns"))

# COMMAND ----------

# MAGIC %md ## Los cortes salen del dato, no del reloj
# MAGIC
# MAGIC Con `current_date()` el cuaderno daría resultados distintos cada día sobre
# MAGIC los mismos datos, y en cuanto el histórico se quedara atrás no habría
# MAGIC ventana de etiqueta y todo el mundo saldría "en fuga".

# COMMAND ----------

ultima = ordenes.agg(F.max(F.to_date("order_date"))).first()[0]

# T2 necesita 90 días de futuro observable para poder etiquetarse; T1, otros 90
# por detrás para que entrenamiento y prueba no compartan periodo.
corte_prueba = ultima - timedelta(days=HORIZONTE)
corte_entrenamiento = corte_prueba - timedelta(days=HORIZONTE)

print(f"Último día con datos      : {ultima}")
print(f"Corte de entrenamiento T1 : {corte_entrenamiento}  (etiqueta hasta {corte_prueba})")
print(f"Corte de prueba        T2 : {corte_prueba}  (etiqueta hasta {ultima})")

# COMMAND ----------

entrenamiento = features.build_training_set(ordenes, devoluciones, cutoff=corte_entrenamiento)
prueba = features.build_training_set(ordenes, devoluciones, cutoff=corte_prueba)


def prevalencia(etiqueta: str, df) -> None:
    fila = df.agg(F.count("*").alias("n"), F.avg("churned").alias("tasa")).first()
    print(f"{etiqueta:<14} {fila['n']:>6,} clientes activos, {fila['tasa']:.1%} en fuga")


prevalencia("Entrenamiento", entrenamiento)
prevalencia("Prueba", prueba)

# COMMAND ----------

# MAGIC %md ## Tres algoritmos contra la regla trivial

# COMMAND ----------

seleccion = churn.select_best(entrenamiento, prueba)

print(f"{'modelo':<26}{'AUC-ROC':>10}{'AUC-PR':>10}")
print("-" * 46)
print(
    f"{'línea base (recencia)':<26}{seleccion.baseline.auc_roc:>10.3f}"
    f"{seleccion.baseline.auc_pr:>10.3f}"
)
for nombre, m in sorted(seleccion.scores.items(), key=lambda par: -par[1].auc_pr):
    marca = "  ←" if nombre == seleccion.algorithm else ""
    print(f"{nombre:<26}{m.auc_roc:>10.3f}{m.auc_pr:>10.3f}{marca}")

ganador = seleccion.scores[seleccion.algorithm]
mejora = ganador.auc_roc - seleccion.baseline.auc_roc
print(f"\nMejora sobre la regla trivial: {mejora:+.3f} de AUC-ROC")
print(f"Prevalencia en prueba: {ganador.positive_rate:.1%} de {ganador.n:,} clientes")

# COMMAND ----------

# MAGIC %md ### En qué se fija el modelo

# COMMAND ----------

for nombre, peso in churn.feature_importances(seleccion.model):
    print(f"{nombre:<26}{peso:>+9.3f}")

# COMMAND ----------

# MAGIC %md ## Predicción accionable
# MAGIC
# MAGIC Las predicciones útiles no son las del conjunto de prueba —esos clientes
# MAGIC ya se fueron o se quedaron—, sino las del **último corte disponible**: los
# MAGIC clientes cuyos 90 días todavía no han pasado. Esas filas no tienen
# MAGIC etiqueta, y ese es justo el punto.

# COMMAND ----------

actuales = features.build_features(ordenes, devoluciones, cutoff=ultima)

# Los tramos salen del **orden** de riesgo, no de umbrales absolutos.
#
# La primera versión cortaba en 0,7 y 0,4, y medirlo la descartó: con una
# prevalencia del 37 % el modelo casi nunca supera 0,7, así que el tramo "alto"
# se quedó con 3 clientes de 914. Un tramo de tres personas no es una lista de
# prioridad, es un redondeo.
#
# Un equipo de retención tiene capacidad fija: llama a los N más probables que
# pueda atender, no a todos los que superen un número. Por eso el corte es por
# decil de riesgo, que es estable ejecución tras ejecución aunque las
# probabilidades se desplacen.
riesgo = F.percent_rank().over(Window.orderBy(F.desc(churn.SCORE_COLUMN)))

predicciones = (
    churn.predict(seleccion.model, actuales)
    .withColumn("_rango", riesgo)
    .withColumn(
        "risk_band",
        F.when(F.col("_rango") < 0.10, "alto")
        .when(F.col("_rango") < 0.30, "medio")
        .otherwise("bajo"),
    )
    .drop("_rango")
    .withColumn("scored_at", F.lit(ultima))
    .withColumn("model", F.lit(seleccion.algorithm))
)

destino = ns.table("gold", "customer_churn_predictions")
predicciones.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)

COMENTARIOS = {
    "customer_id": "Identificador del cliente",
    "churn_probability": f"Probabilidad de no volver a comprar en {HORIZONTE} días",
    "risk_band": "Tramo por decil de riesgo: alto (10% mas probable), medio (20% siguiente), bajo",
    "scored_at": "Corte con el que se calcularon las variables",
    "model": "Algoritmo ganador en la validacion fuera de tiempo",
    "recency_days": "Días desde la última compra hasta el corte",
    "frequency": "Órdenes con ingreso reconocido hasta el corte",
    "monetary": "Gasto acumulado hasta el corte",
    "avg_order_value": "Ticket medio hasta el corte",
    "tenure_days": "Días desde la primera compra",
    "orders_last_30d": "Órdenes en los 30 días previos al corte",
    "orders_last_90d": "Órdenes en los 90 días previos al corte",
    "spend_last_90d": "Gasto en los 90 días previos al corte",
    "avg_days_between_orders": "Intervalo medio entre compras",
    "return_rate": "Porcentaje de órdenes con devolución",
}
for columna, comentario in COMENTARIOS.items():
    spark.sql(f"ALTER TABLE {destino} ALTER COLUMN {columna} COMMENT '{comentario}'")

display(
    spark.table(destino)
    .groupBy("risk_band")
    .agg(F.count("*").alias("clientes"), F.avg(churn.SCORE_COLUMN).alias("prob_media"))
    .orderBy("risk_band")
)

# COMMAND ----------

# MAGIC %md ## Las métricas también son un dato
# MAGIC
# MAGIC Guardarlas en una tabla permite comparar reentrenamientos y detectar
# MAGIC degradación. Un número que solo vive en la salida de un cuaderno se pierde
# MAGIC en cuanto alguien vuelve a ejecutarlo.

# COMMAND ----------

filas = [("baseline_recency", corte_entrenamiento, corte_prueba, seleccion.baseline)] + [
    (nombre, corte_entrenamiento, corte_prueba, m) for nombre, m in seleccion.scores.items()
]

metricas = spark.createDataFrame(
    [
        (
            nombre,
            t1,
            t2,
            HORIZONTE,
            m.auc_roc,
            m.auc_pr,
            m.n,
            m.positive_rate,
            nombre == seleccion.algorithm,
        )
        for nombre, t1, t2, m in filas
    ],
    "model string, train_cutoff date, test_cutoff date, horizon_days int, "
    "auc_roc double, auc_pr double, test_rows bigint, positive_rate double, selected boolean",
).withColumn("evaluated_at", F.current_timestamp())

metricas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    ns.table("ops", "model_metrics")
)
display(spark.table(ns.table("ops", "model_metrics")).orderBy(F.desc("auc_pr")))
