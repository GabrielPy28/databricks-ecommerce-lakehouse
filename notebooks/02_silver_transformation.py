# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — tipado, cuarentena y carga idempotente
# MAGIC
# MAGIC Tres cosas ocurren aquí:
# MAGIC
# MAGIC 1. **Tipado con `try_cast`.** Un valor inválido se vuelve nulo en lugar de
# MAGIC    abortar el job. Con el modo ANSI de Spark 4, un `cast` normal tumbaría
# MAGIC    el pipeline entero por un solo registro corrupto entre millones.
# MAGIC 2. **Cuarentena.** Las filas cuya conversión falló no se descartan: se
# MAGIC    apartan junto con la columna concreta que falló, y son reprocesables.
# MAGIC 3. **MERGE con desempate por `updated_at`.** De ahí salen la idempotencia
# MAGIC    y el tratamiento de datos tardíos.
# MAGIC
# MAGIC Toda la lógica vive en `src/ecommerce/transformations` y está cubierta por
# MAGIC tests que corren en el contenedor. Este notebook solo la encadena.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text(
    "entities",
    "customers,products,orders,order_items,payments,reviews,returns,web_events",
    "Entidades",
)
dbutils.widgets.text("run_id", "", "Identificador de ejecución")

# COMMAND ----------

import uuid
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from ecommerce import config, schemas
from ecommerce.quality import engine as calidad
from ecommerce.quality import report as reporte
from ecommerce.quality import rules as reglas_calidad
from ecommerce.schemas.spark import to_spark_schema
from ecommerce.transformations import cleaning, merge, scd2, silver

# Dimensiones con historial. Un cambio de país o de precio abre una versión
# nueva en lugar de sobrescribir la anterior.
DIMENSIONES_HISTORIFICADAS = {"customers", "products"}

ns = config.Namespace()
entidades = [e.strip() for e in dbutils.widgets.get("entities").split(",") if e.strip()]

run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
reglas_por_tabla = reglas_calidad.load_rules()
resultados_calidad = []

# Línea base de volumen: cuántas filas comprobó `row_count_delta` la última vez.
#
# Sale del propio histórico de calidad, no de un fichero aparte: `quality_results`
# ya registra lo medido en cada ejecución, y usarlo como referencia mantiene una
# sola fuente de verdad. Sin filas previas el diccionario queda vacío y la regla
# se limita a informar, que es lo correcto en la primera ejecución.
TABLA_CALIDAD = ns.table("ops", "quality_results")

lineas_base: dict[str, int] = {}
if spark.catalog.tableExists(TABLA_CALIDAD):
    historico = spark.table(TABLA_CALIDAD).filter("rule = 'row_count_delta'")
    ultima = Window.partitionBy("table").orderBy(F.col("checked_at").desc())
    lineas_base = {
        fila["table"]: fila.rows_checked
        for fila in historico.withColumn("_rn", F.row_number().over(ultima))
        .filter("_rn = 1")
        .collect()
    }
    print(f"Líneas base de volumen: {len(lineas_base)} tabla(s)")

# Columnas de texto libre que conviene recortar en todas las entidades.
COLUMNAS_A_RECORTAR = {"order_id", "customer_id", "product_id", "order_item_id", "email"}

# COMMAND ----------


def crear_si_no_existe(nombre: str, entidad: str, con_errores: bool) -> None:
    """Crea la tabla vacía con el esquema del contrato.

    El MERGE necesita un destino existente. Crearla desde el contrato —y no
    dejar que la infiera la primera escritura— garantiza que los tipos sean los
    acordados aunque el primer lote venga incompleto.
    """
    esquema = to_spark_schema(entidad)
    columnas = [f"`{c.name}` {c.dataType.simpleString()}" for c in esquema.fields]
    if con_errores:
        columnas.append(f"`{silver.ERRORS}` array<string>")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {nombre} ({', '.join(columnas)}) USING DELTA")


def crear_scd2_si_no_existe(nombre: str, entidad: str) -> None:
    """Crea la tabla historificada: contrato sin `updated_at`, más vigencia.

    `updated_at` no se guarda como columna propia: su información pasa a
    `valid_from`. Conservar ambas invitaría a que divergieran.
    """
    esquema = to_spark_schema(entidad)
    columnas = [
        f"`{c.name}` {c.dataType.simpleString()}" for c in esquema.fields if c.name != "updated_at"
    ]
    columnas += [
        f"`{scd2.VALID_FROM}` timestamp",
        f"`{scd2.VALID_TO}` timestamp",
        f"`{scd2.IS_CURRENT}` boolean",
    ]
    spark.sql(f"CREATE TABLE IF NOT EXISTS {nombre} ({', '.join(columnas)}) USING DELTA")


# COMMAND ----------

for entidad in entidades:
    origen = spark.table(ns.table("bronze", f"{entidad}_raw"))
    clave = schemas.PRIMARY_KEYS[entidad]

    # Normalización antes de convertir: ` DELIVERED ` es un estado legítimo mal
    # escrito, y rechazarlo por la forma en vez de por el contenido sería un
    # falso positivo de la validación.
    df = cleaning.trim_strings(origen, sorted(COLUMNAS_A_RECORTAR & set(origen.columns)))
    if "status" in df.columns:
        df = cleaning.normalize_status(df, "status")

    tipado = cleaning.with_cast_errors(df, entidad)

    # La deduplicación va ANTES de validar, y no es un detalle de orden.
    #
    # Un duplicado reemitido por el sistema de origen es una condición conocida
    # que Silver sabe resolver: no es una fila inválida. Si se validara primero,
    # la regla `unique` marcaría como incumplimiento las 60 filas implicadas en
    # los 30 duplicados —y los apartaría a cuarentena— cuando lo correcto es
    # quedarse con la versión más reciente de cada una.
    #
    # Tras esto, `unique` queda como red de seguridad: no debería saltar nunca,
    # y si salta es que algo va mal de verdad.
    #
    # Nota: no se usa `.cache()` para evitar recalcular. El cómputo serverless
    # de Databricks lo rechaza con NOT_SUPPORTED_WITH_SERVERLESS, así que el
    # recuento se paga dos veces a propósito.
    #
    # En las dimensiones registradas la clave de deduplicación incluye
    # `updated_at`. Agrupar solo por la clave de negocio conservaría la versión
    # más reciente y **destruiría el historial**: la dimensión acabaría con una
    # sola versión por clave y SCD2 no serviría para nada, sin que nada fallara.
    clave_dedup = [clave, "updated_at"] if entidad in DIMENSIONES_HISTORIFICADAS else clave
    antes = tipado.count()
    tipado = merge.deduplicate_latest(tipado, key=clave_dedup, order_by=schemas.ORDER_BY[entidad])
    duplicados = antes - tipado.count()

    # Las reglas se evalúan aquí, sobre las filas candidatas, y no sobre la
    # tabla ya construida. Si se aplicaran después, el MERGE volvería a
    # insertar en cada ejecución las mismas filas que la calidad acaba de
    # apartar, y el pipeline entraría en un bucle silencioso.
    #
    # Las referencias de clave ajena apuntan a las tablas Silver ya cargadas.
    # De ahí que el orden de `entities` importe: las dimensiones antes que los
    # hechos que las referencian.
    nombre_reglas = f"silver.{entidad}"
    reglas = reglas_por_tabla.get(nombre_reglas, [])
    referencias = {
        regla.params["ref"]: spark.table(ns.table("silver", regla.params["ref"].split(".")[-1]))
        for regla in reglas
        if regla.rule == "foreign_key"
        and spark.catalog.tableExists(ns.table("silver", regla.params["ref"].split(".")[-1]))
    }
    marcado, resultados = calidad.evaluate(tipado, reglas, referencias, lineas_base)
    resultados_calidad.extend(resultados)

    validas, cuarentena = silver.split_quarantine(marcado)

    # Solo las columnas del contrato: los metadatos de linaje se quedan en
    # Bronze, que es donde tienen sentido.
    columnas = [c.name for c in to_spark_schema(entidad).fields]
    validas = validas.select(*columnas)

    tabla = ns.table("silver", entidad)

    # La cuarentena se reescribe entera, no se acumula. Silver recorre todo
    # Bronze en cada ejecución, así que la cuarentena es una función
    # determinista de lo ingerido y reconstruirla es correcto; con `append`,
    # reejecutar duplicaría cada fila rechazada y el reporte mentiría.
    tabla_cuarentena = ns.table("silver", f"{entidad}_quarantine")
    crear_si_no_existe(tabla_cuarentena, entidad, con_errores=True)
    (
        cuarentena.select(*columnas, silver.ERRORS)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_cuarentena)
    )
    n_cuarentena = spark.table(tabla_cuarentena).count()

    if entidad in DIMENSIONES_HISTORIFICADAS:
        # SCD Tipo 2: un cambio de atributo cierra la versión vigente y abre
        # una nueva, en vez de sobrescribir. Sin esto, un cliente que se muda
        # reescribiría retroactivamente los ingresos históricos de los dos
        # países implicados.
        crear_scd2_si_no_existe(tabla, entidad)
        # `apply_history` y no `apply`: el origen trae todas las versiones
        # acumuladas en Bronze y hay que reproducirlas en orden cronológico.
        # Aplicarlas de golpe es imposible —el MERGE exige una fila por clave— y
        # quedarse con la última destruiría el historial.
        scd2.apply_history(
            DeltaTable.forName(spark, tabla), validas, key=clave, effective_from="updated_at"
        )
        versiones = spark.table(tabla).count()
        vigentes = spark.table(tabla).filter(scd2.IS_CURRENT).count()
        print(
            f"{entidad:<14} versiones={versiones:>8,}   vigentes={vigentes:>8,}   "
            f"cuarentena={n_cuarentena:>6,}   reglas={len(reglas)}"
        )
        continue

    crear_si_no_existe(tabla, entidad, con_errores=False)

    # El desempate lo declara el contrato, no lo adivina el notebook: las
    # entidades que mutan usan `updated_at`, las inmutables su propia clave.
    desempate = schemas.ORDER_BY[entidad]

    merge.merge_latest(DeltaTable.forName(spark, tabla), validas, key=clave, order_by=desempate)

    # Retirar de Silver lo que hoy la calidad rechaza.
    #
    # El MERGE solo inserta y actualiza: nunca borra. Sin este paso, una fila
    # que entró antes de que existiera la regla que ahora la rechaza se queda
    # publicada para siempre, y Gold la sigue contando.
    #
    # Ocurrió de verdad: 10 órdenes con cliente inexistente entraron en Silver
    # antes de que el motor de calidad existiera, y seguían ahí —contadas en los
    # ingresos— aunque la cuarentena las marcaba. Silver debe reflejar lo que
    # las reglas de hoy aceptan, no lo que aceptaban las de ayer.
    #
    # Es idempotente: la cuarentena es una función determinista de Bronze.
    spark.sql(f"""
        DELETE FROM {tabla}
        WHERE {clave} IN (SELECT {clave} FROM {tabla_cuarentena})
    """)

    print(
        f"{entidad:<14} válidas={spark.table(tabla).count():>8,}   "
        f"cuarentena={n_cuarentena:>6,}   "
        f"duplicados_eliminados={duplicados:>5,}   reglas={len(reglas)}"
    )

# COMMAND ----------

# Los resultados van a una tabla, no solo al log: un reporte que se imprime se
# pierde al cerrar la ejecución, y sin histórico no se puede ver si la calidad
# mejora o empeora con el tiempo.
filas = reporte.to_rows(
    resultados_calidad, run_id=run_id, checked_at=datetime.now(UTC).isoformat(timespec="seconds")
)
destino_calidad = ns.table("ops", "quality_results")
(
    spark.createDataFrame(filas)
    .write.mode("append")
    # `mergeSchema` porque la tabla evoluciona: `detail` se añadió para que las
    # reglas de nivel de tabla puedan explicarse. Sin esto habría que borrarla y
    # se perdería el histórico, que es justo lo que da valor a esta tabla.
    .option("mergeSchema", "true")
    .saveAsTable(destino_calidad)
)

print(f"{len(filas)} resultados escritos en {destino_calidad} (ejecución {run_id})")
