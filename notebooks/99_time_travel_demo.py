# Databricks notebook source
# MAGIC %md
# MAGIC # Time Travel: recuperarse de una transformación incorrecta
# MAGIC
# MAGIC Esta demostración es **reproducible y destructiva por diseño**: rompe una
# MAGIC tabla Gold a propósito, enseña el número equivocado que produce, y la
# MAGIC recupera. Se ejecuta sobre `gold.daily_sales`, que el pipeline reconstruye
# MAGIC entera, así que no hay nada que perder.
# MAGIC
# MAGIC El escenario que simula es el que ocurre de verdad: alguien despliega una
# MAGIC transformación con un error de signo o de filtro, el job **termina con
# MAGIC éxito**, y el fallo se descubre horas después mirando un dashboard. No hay
# MAGIC excepción que capturar ni alerta que salte: solo una cifra que no cuadra.
# MAGIC
# MAGIC Sin historial de versiones la única salida sería reconstruir desde Bronze,
# MAGIC que en un lakehouse real puede ser horas de cómputo. Con Delta es una
# MAGIC sentencia.
# MAGIC
# MAGIC No es un `%md` con capturas: cada celda se ejecuta y las cifras que
# MAGIC aparecen son las de esta ejecución.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

from ecommerce import config

ns = config.Namespace()
TABLA = ns.table("gold", "daily_sales")


def ingreso_total() -> float:
    return spark.table(TABLA).agg(F.sum("net_revenue").alias("t")).first().t


def version_actual() -> int:
    return spark.sql(f"DESCRIBE HISTORY {TABLA} LIMIT 1").first().version


# COMMAND ----------

# MAGIC %md ## 1. El estado correcto, antes de romper nada

# COMMAND ----------

version_buena = version_actual()
ingreso_bueno = ingreso_total()

# Reconciliación contra Silver ANTES de romper nada.
#
# No es ceremonia: la primera versión de este notebook no la tenía, falló a
# mitad de camino dejando la tabla inflada, y la siguiente ejecución tomó ese
# estado corrupto como "el bueno" y restauró a él. La demo reprodujo el
# incidente que pretende enseñar a resolver.
#
# Partir de un estado verificado es lo que hace que "restaurar" signifique algo.
ingreso_silver = (
    spark.table(ns.table("silver", "orders"))
    .filter("status IN ('paid', 'shipped', 'delivered')")
    .agg(F.sum("total_amount").alias("t"))
    .first()
    .t
)

print(f"Tabla:            {TABLA}")
print(f"Versión:          {version_buena}")
print(f"Ingreso en Gold:  {ingreso_bueno:,.2f}")
print(f"Ingreso en Silver:{ingreso_silver:,.2f}")

assert abs(float(ingreso_bueno) - float(ingreso_silver)) < 0.01, (
    f"Gold no reconcilia con Silver antes de empezar "
    f"({ingreso_bueno} != {ingreso_silver}). La tabla ya está corrupta: "
    f"reconstruye Gold antes de ejecutar esta demostración, o restaurarías a "
    f"un estado igualmente incorrecto."
)
print("\nEstado de partida verificado contra Silver.")

# COMMAND ----------

# MAGIC %md ## 2. Romper, diagnosticar y recuperar
# MAGIC
# MAGIC Todo en una sola celda, con `try`/`finally`. **No es un detalle de
# MAGIC estilo.** La primera versión de este notebook repartía estos pasos en
# MAGIC celdas separadas; una de ellas falló, la ejecución se detuvo entre romper
# MAGIC y restaurar, y la tabla se quedó inflada. La siguiente ejecución tomó ese
# MAGIC estado como "el bueno".
# MAGIC
# MAGIC Una demostración destructiva que puede abortar a medias debe garantizar
# MAGIC la restauración pase lo que pase.
# MAGIC
# MAGIC El error simulado es de los más fáciles de cometer y de los más difíciles
# MAGIC de ver: un factor aplicado a los ingresos de un rango de fechas. El
# MAGIC `UPDATE` se ejecuta sin quejarse y el dashboard publica la cifra inflada.

# COMMAND ----------

try:
    # --- Se despliega la transformación con el error ---
    spark.sql(f"""
        UPDATE {TABLA}
        SET net_revenue = net_revenue * 1.15,
            gross_revenue = gross_revenue * 1.15
        WHERE date >= '2026-01-01'
    """)

    version_rota = version_actual()
    ingreso_roto = ingreso_total()

    print(f"Versión tras el despliegue: {version_rota}")
    print(f"Ingreso ahora:              {ingreso_roto:,.2f}")
    print(f"Desviación:                 {ingreso_roto - ingreso_bueno:+,.2f}")
    print()
    print("El job terminó con éxito. Nada falló. El dashboard ya muestra esta cifra.")

    # --- Diagnóstico: qué cambió y cuánto ---
    #
    # Delta permite consultar una versión anterior como si fuera otra tabla, así
    # que cuantificar el daño antes de tocar nada es una consulta normal.
    #
    # Se consulta por nombre y no por ruta: en Free Edition con Default Storage,
    # `DESCRIBE DETAIL` devuelve `location` vacío y `.load(ruta)` falla con
    # "Can not create a Path from an empty string".
    antes = spark.sql(f"SELECT * FROM {TABLA} VERSION AS OF {version_buena}")

    comparacion = (
        antes.alias("a")
        .join(spark.table(TABLA).alias("d"), on="date", how="full_outer")
        .select(
            "date",
            F.col("a.net_revenue").alias("correcto"),
            F.col("d.net_revenue").alias("publicado"),
            (F.col("d.net_revenue") - F.col("a.net_revenue")).alias("desviacion"),
        )
        .filter(F.col("desviacion") != 0)
    )

    print(f"\nDías afectados: {comparacion.count():,}")
    display(comparacion.orderBy(F.col("desviacion").desc()).limit(10))

finally:
    # --- Recuperación, ocurra lo que ocurra arriba ---
    #
    # `RESTORE` no borra el historial: añade una versión nueva cuyo contenido es
    # el de la anterior. La versión rota sigue ahí, lo que importa para una
    # auditoría: se puede demostrar qué se publicó y durante cuánto tiempo.
    spark.sql(f"RESTORE TABLE {TABLA} TO VERSION AS OF {version_buena}")
    print(f"\nRestaurada a la versión {version_buena}.")

# COMMAND ----------

# MAGIC %md ## 3. Verificación
# MAGIC
# MAGIC La comprobación que cierra el ciclo: el dato volvió a ser correcto y el
# MAGIC historial conserva todas las versiones, así que el incidente queda
# MAGIC documentado en la propia tabla.

# COMMAND ----------

version_restaurada = version_actual()
ingreso_restaurado = ingreso_total()

assert abs(float(ingreso_restaurado) - float(ingreso_bueno)) < 0.01, (
    f"La restauración no devolvió el valor original: {ingreso_restaurado} != {ingreso_bueno}"
)

print("Recuperación verificada.\n")
print(f"  versión {version_buena:>3}  correcta     {ingreso_bueno:>15,.2f}")
print(f"  versión {version_restaurada:>3}  restaurada   {ingreso_restaurado:>15,.2f}")
print()
print("La versión rota sigue en el historial: se puede auditar qué se publicó")
print("y durante cuánto tiempo, en lugar de que el incidente desaparezca.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT version, timestamp, operation
        FROM (DESCRIBE HISTORY {TABLA})
        ORDER BY version DESC
        LIMIT 6
    """)
)
