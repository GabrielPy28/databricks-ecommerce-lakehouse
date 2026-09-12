"""Dimensiones de cambio lento, Tipo 2.

Guarda el historial de una dimensión en lugar de solo su estado actual. Cada
versión de una fila lleva su intervalo de vigencia:

    customer_id  country  valid_from   valid_to     is_current
    CUS-1        MX       2026-01-01   2026-03-01   false
    CUS-1        ES       2026-03-01   NULL         true

**Por qué hace falta,** si `order_items.unit_price` ya conserva el precio de
venta: los ingresos históricos salen bien sin SCD2, pero el análisis por
segmento no. Un cliente que se muda de México a España reescribiría
retroactivamente los ingresos históricos de ambos países si la dimensión solo
guardara dónde está hoy.

La aplicación son dos pasadas de MERGE:

1. Cerrar las versiones vigentes cuyos atributos cambiaron.
2. Insertar una versión nueva para toda clave que ya no tenga vigente.

El orden importa: tras la primera pasada, una clave que cambió se queda sin
fila vigente, y la segunda la reconoce como pendiente de insertar. Las claves
sin cambios conservan la suya y no se tocan, lo que hace la operación
idempotente sin necesidad de comparar nada más.
"""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

VALID_FROM = "valid_from"
VALID_TO = "valid_to"
IS_CURRENT = "is_current"

HISTORY_COLUMNS = (VALID_FROM, VALID_TO, IS_CURRENT)


def _atributos(df: DataFrame, key: str, effective_from: str) -> list[str]:
    """Columnas cuyo cambio abre una versión nueva."""
    excluidas = {key, effective_from, *HISTORY_COLUMNS}
    return [columna for columna in df.columns if columna not in excluidas]


def with_history(df: DataFrame, *, effective_from: str) -> DataFrame:
    """Añade las columnas de vigencia a un DataFrame plano."""
    return (
        df.withColumn(VALID_FROM, F.col(effective_from))
        .withColumn(VALID_TO, F.lit(None).cast("timestamp"))
        .withColumn(IS_CURRENT, F.lit(True))
        .drop(effective_from)
    )


def initialize(df: DataFrame, path: str, *, key: str, effective_from: str) -> None:
    """Crea la tabla SCD2 a partir de una foto inicial."""
    del key  # La foto inicial no necesita comparar: todo entra como vigente.
    with_history(df, effective_from=effective_from).write.format("delta").save(path)


def _difieren(atributos: list[str]) -> str:
    """Condición SQL: algún atributo cambió entre destino y origen.

    Usa `<=>` (igualdad segura ante nulos) y no `=`. Con `=`, comparar `'MX'`
    contra `NULL` da `NULL`, que no es cierto, y un atributo que pasa a nulo
    —o que deja de serlo— no se detectaría como cambio.
    """
    return " OR ".join(f"NOT (t.`{a}` <=> s.`{a}`)" for a in atributos)


def apply(
    target: DeltaTable,
    source: DataFrame,
    *,
    key: str,
    attributes: list[str] | None = None,
    effective_from: str,
) -> None:
    """Aplica un lote de cambios a la dimensión historificada."""
    atributos = attributes or _atributos(source, key, effective_from)
    difieren = _difieren(atributos)

    # Solo se aplica lo estrictamente posterior a la versión vigente.
    #
    # Sin esta guarda, reprocesar una versión antigua cerraría la vigente con
    # una fecha del pasado y la reabriría hacia atrás: el historial crecería
    # con versiones falsas en cada reejecución. Es la misma condición que hace
    # idempotente el MERGE de los hechos, y aquí además protege de las
    # dimensiones que llegan tarde.
    posterior = f"s.`{effective_from}` > t.{VALID_FROM}"

    # Pasada 1: cerrar las versiones vigentes que cambiaron.
    (
        target.alias("t")
        .merge(source.alias("s"), f"t.`{key}` = s.`{key}` AND t.{IS_CURRENT}")
        .whenMatchedUpdate(
            condition=f"({difieren}) AND {posterior}",
            set={
                IS_CURRENT: F.lit(False),
                # El cierre coincide exactamente con la apertura de la versión
                # siguiente: sin huecos ni solapes, para que unir por fecha no
                # pierda ni duplique filas.
                VALID_TO: F.col(f"s.`{effective_from}`"),
            },
        )
        .execute()
    )

    # Pasada 2: abrir versión para toda clave sin vigente.
    #
    # Son dos casos: las que se acaban de cerrar y las que nunca existieron. No
    # hace falta distinguirlos, y no distinguirlos es lo que mantiene el código
    # simple y la operación idempotente.
    nuevas = with_history(source, effective_from=effective_from)
    (
        target.alias("t")
        .merge(nuevas.alias("s"), f"t.`{key}` = s.`{key}` AND t.{IS_CURRENT}")
        .whenNotMatchedInsertAll()
        .execute()
    )


def apply_history(
    target: DeltaTable,
    source: DataFrame,
    *,
    key: str,
    attributes: list[str] | None = None,
    effective_from: str,
) -> None:
    """Reproduce en orden cronológico todas las versiones del origen.

    Silver recorre todo Bronze en cada ejecución, y Bronze acumula una fila por
    cada vez que la dimensión cambió. Ese origen no se puede aplicar de golpe:
    el MERGE exige una sola fila por clave.

    Quedarse con la más reciente —que es lo que hace la deduplicación normal—
    **destruye el historial**: la dimensión acaba con una única versión por
    clave y SCD2 deja de servir para nada, sin que nada falle.

    Reproducirlas en orden reconstruye el historial completo desde Bronze, y
    hacerlo dos veces no cambia nada, porque una versión que ya existe no
    difiere de la almacenada y no abre ninguna nueva.

    **El bucle va por número de versión dentro de cada clave, no por marca de
    tiempo.** La diferencia no es cosmética: lo que obliga a separar pasadas es
    que una misma clave traiga varias versiones, porque el MERGE exige una fila
    por clave. Filas de claves distintas caben en la misma pasada aunque sus
    fechas difieran.

    Agrupar por fecha parecía equivalente y no lo es. En cuanto la dimensión
    pasó a marcar cada cliente con su fecha de alta —una marca distinta por
    fila—, la carga inicial de mil clientes lanzaba unos dos mil MERGE y Silver
    pasó de tres minutos a más de cincuenta. Por versión, son dos pasadas.

    Así las iteraciones son el máximo de versiones que trae una sola clave: dos
    con dos lotes, treinta y una con treinta, independientemente del número de
    filas o de fechas distintas.
    """
    rango = "_scd2_rango"
    por_version = source.withColumn(
        rango,
        F.row_number().over(Window.partitionBy(key).orderBy(F.col(effective_from).asc())),
    )

    maximo = por_version.agg(F.max(rango)).first()[0] or 0
    for version in range(1, maximo + 1):
        apply(
            target,
            por_version.filter(F.col(rango) == version).drop(rango),
            key=key,
            attributes=attributes,
            effective_from=effective_from,
        )


def join_as_of(
    facts: DataFrame,
    dimension: DataFrame,
    *,
    key: str,
    timestamp_column: str,
) -> DataFrame:
    """Une cada hecho con la versión de la dimensión vigente en su fecha.

    Es para lo que sirve el historial. Uniendo contra la versión actual, una
    compra de enero en México aparecería atribuida a España solo porque el
    cliente se mudó en marzo, y los ingresos históricos por país cambiarían
    cada vez que alguien se muda.

    La unión es por la izquierda: un hecho de una clave que no existe en la
    dimensión, o anterior a su primera versión, se conserva con los atributos a
    nulo. Descartarlo perdería ingresos en silencio, que es peor que
    publicarlos sin atributos.

    Las columnas de vigencia no salen en el resultado: son mecánica interna del
    historial y en una tabla Gold solo añadirían ruido.
    """
    hechos = facts.alias("f")
    dim = dimension.alias("d")

    condicion = (
        (F.col(f"f.`{key}`") == F.col(f"d.`{key}`"))
        & (F.col(f"d.{VALID_FROM}") <= F.col(f"f.`{timestamp_column}`"))
        & (
            F.col(f"d.{VALID_TO}").isNull()
            | (F.col(f"d.{VALID_TO}") > F.col(f"f.`{timestamp_column}`"))
        )
    )

    # Solo los atributos de la dimensión, sin su clave (ya está en los hechos)
    # ni sus columnas de vigencia.
    atributos = [
        F.col(f"d.`{columna}`")
        for columna in dimension.columns
        if columna != key and columna not in HISTORY_COLUMNS
    ]
    return hechos.join(dim, condicion, "left").select("f.*", *atributos)


def current(df: DataFrame) -> DataFrame:
    """Solo las versiones vigentes: la dimensión como se vería sin historial."""
    return df.filter(F.col(IS_CURRENT))


def as_of(df: DataFrame, momento: datetime | str | Column) -> DataFrame:
    """La dimensión tal como era en un instante dado.

    Es para lo que existe SCD2: saber en qué país estaba el cliente cuando hizo
    aquella compra, no dónde vive hoy.

    El intervalo es cerrado por la izquierda y abierto por la derecha
    —`valid_from <= momento < valid_to`—, de modo que en el instante exacto del
    cambio la versión vigente es la nueva y no ambas.
    """
    instante = momento if isinstance(momento, Column) else F.lit(momento).cast("timestamp")
    return df.filter(
        (F.col(VALID_FROM) <= instante) & (F.col(VALID_TO).isNull() | (F.col(VALID_TO) > instante))
    )
