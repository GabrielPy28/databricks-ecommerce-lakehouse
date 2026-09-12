# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — ingesta con Auto Loader
# MAGIC
# MAGIC **Principio de la capa: preservar lo que llegó.** Ninguna regla de
# MAGIC negocio, ningún tipado, ninguna fila descartada. Solo se añaden metadatos
# MAGIC de linaje, que no alteran el dato: lo explican.
# MAGIC
# MAGIC El lote de cada fila **se deduce de la ruta del fichero**, no de un
# MAGIC parámetro: Auto Loader ingiere todos los ficheros nuevos del directorio
# MAGIC sin saber con qué argumentos se invocó el job, así que etiquetar con el
# MAGIC parámetro produce etiquetas que mienten.
# MAGIC
# MAGIC La idempotencia no se programa: la aporta el checkpoint de Auto Loader,
# MAGIC que lleva su propio registro de qué ficheros ya procesó. Reejecutar este
# MAGIC notebook sobre un lote ya ingerido no añade una sola fila.
# MAGIC
# MAGIC Se usa `trigger(availableNow=True)`: semántica de streaming —control de
# MAGIC progreso, tolerancia a fallos, reanudación— pero procesando lo disponible
# MAGIC y terminando, sin consumo continuo de cuota.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Identificador de ejecución")

# COMMAND ----------

import uuid

from ecommerce import config, schemas
from ecommerce.ingestion import autoloader, bronze

ns = config.Namespace()

# Enlaza cada fila con la ejecución que la escribió. Si no se recibe uno, se
# genera: una fila sin ejecución asociada es una fila que no se puede rastrear.
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

print(f"Ejecución: {run_id}")
print(f"Origen: {ns.volume_root()}")

# COMMAND ----------

for entidad in schemas.ALL_SCHEMAS:
    origen = f"{ns.volume_root()}/{entidad}"
    destino = ns.table("bronze", f"{entidad}_raw")
    checkpoint = f"{ns.checkpoints_root()}/bronze/{entidad}"

    df = autoloader.read_stream(
        spark,
        source_path=origen,
        entity=entidad,
        schema_location=f"{ns.checkpoints_root()}/schemas",
    )

    consulta = (
        bronze.add_lineage(df, run_id=run_id)
        .writeStream.option("checkpointLocation", checkpoint)
        # `append` y nunca `overwrite`: Bronze es un registro histórico
        # acumulativo, no una foto del último lote.
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(destino)
    )
    consulta.awaitTermination()

    filas = spark.table(destino).count()
    print(f"{entidad:<14} -> {destino:<40} {filas:>10,} filas acumuladas")
