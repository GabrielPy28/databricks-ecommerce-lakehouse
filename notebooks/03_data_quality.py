# Databricks notebook source
# MAGIC %md
# MAGIC # Puerta de calidad
# MAGIC
# MAGIC Se sitúa entre Silver y Gold. Lee los resultados que escribió el paso de
# MAGIC Silver, publica el reporte y **detiene el pipeline** si se incumplió
# MAGIC alguna regla de severidad `fail`.
# MAGIC
# MAGIC Esa parada es la razón de que exista como paso aparte. Sin ella, Gold
# MAGIC publicaría cifras construidas sobre datos que no cumplen lo mínimo —una
# MAGIC clave primaria nula o repetida— y el error se descubriría en un
# MAGIC dashboard, no en un log.
# MAGIC
# MAGIC Las reglas viven en `src/ecommerce/quality/rules.yml`. Añadir una
# MAGIC comprobación no requiere tocar este notebook.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Ejecución a evaluar (vacío = la última)")

# COMMAND ----------

from pyspark.sql import functions as F

from ecommerce import config
from ecommerce.quality.engine import Outcome
from ecommerce.quality.report import format_report

ns = config.Namespace()
tabla = ns.table("ops", "quality_results")

# COMMAND ----------

run_id = dbutils.widgets.get("run_id")
if not run_id:
    run_id = spark.table(tabla).agg(F.max("checked_at").alias("m")).first().m
    run_id = spark.table(tabla).filter(F.col("checked_at") == run_id).first().run_id

resultados = [
    Outcome(
        table=f.table,
        column=f.column,
        rule=f.rule,
        severity=f.severity,
        rows_checked=f.rows_checked,
        rows_failed=f.rows_failed,
    )
    for f in spark.table(tabla)
    .filter(F.col("run_id") == run_id)
    .orderBy("table", "column")
    .collect()
]

# Sin resultados no hay nada que evaluar, y aprobar en ese caso sería el peor
# fallo posible de una puerta de calidad: daría vía libre precisamente cuando
# el paso anterior no llegó a ejecutarse. Se detiene.
if not resultados:
    raise AssertionError(
        f"Puerta de calidad: no hay resultados para la ejecución '{run_id}' en {tabla}. "
        f"El paso de Silver no llegó a escribirlos, así que no hay nada que garantizar."
    )

print(format_report(resultados))

# COMMAND ----------

bloqueantes = [r for r in resultados if r.severity == "fail" and not r.passed]

if bloqueantes:
    detalle = ", ".join(
        f"{r.table}.{r.column} {r.rule} ({r.rows_failed:,} filas)" for r in bloqueantes
    )
    # Se detiene con excepción y no con un aviso: el objetivo es que el job
    # falle y las tareas siguientes no se ejecuten.
    raise AssertionError(f"Puerta de calidad: {len(bloqueantes)} regla(s) bloqueante(s). {detalle}")

print("\nPuerta de calidad superada: ninguna regla de severidad 'fail' incumplida.")
