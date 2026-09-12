"""Ejecutor de reglas de calidad.

Un único ejecutor aplica todas las reglas a todas las tablas. Añadir una regla
nueva es añadir una entrada a `_PREDICADOS` y una línea a `rules.yml`.

Cada regla se expresa como un predicado que vale `True` cuando la fila
**incumple**. Esa uniformidad es lo que permite contar, marcar y reportar sin
casos especiales.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ecommerce.quality.rules import Rule

RULE_ERRORS = "_rule_errors"


@dataclass(frozen=True)
class Outcome:
    """Resultado de aplicar una regla a una tabla.

    `detail` explica el resultado cuando el recuento no basta. Una regla por
    fila se explica sola —«10 de 10.000 incumplen»—, pero una de tabla no: sin
    decir de cuántas filas a cuántas, «row_count_delta falló» no permite actuar.
    """

    table: str
    column: str
    rule: str
    severity: str
    rows_checked: int
    rows_failed: int
    detail: str = ""

    @property
    def pass_rate(self) -> float:
        # Una tabla vacía aprueba: no hay ninguna fila que incumpla nada.
        # Dividir entre cero aquí tumbaría el pipeline en el primer lote vacío.
        if self.rows_checked == 0:
            return 100.0
        return 100.0 * (self.rows_checked - self.rows_failed) / self.rows_checked

    @property
    def passed(self) -> bool:
        return self.rows_failed == 0


def _not_null(columna: Column, _regla: Rule, _refs: dict, _ambito: Column) -> Column:
    return columna.isNull()


def _in_set(columna: Column, regla: Rule, _refs: dict, _ambito: Column) -> Column:
    valores = regla.params["values"]
    # El nulo no se cuenta aquí: de la ausencia se ocupa `not_null`. Contarlo
    # en las dos reglas inflaría el recuento de incumplimientos.
    return columna.isNotNull() & ~columna.isin(*valores)


def _min_value(columna: Column, regla: Rule, _refs: dict, _ambito: Column) -> Column:
    return columna.isNotNull() & (columna < F.lit(regla.params["value"]))


def _max_value(columna: Column, regla: Rule, _refs: dict, _ambito: Column) -> Column:
    return columna.isNotNull() & (columna > F.lit(regla.params["value"]))


def _matches(columna: Column, regla: Rule, _refs: dict, _ambito: Column) -> Column:
    return columna.isNotNull() & ~columna.rlike(regla.params["pattern"])


def _unique(columna: Column, regla: Rule, _refs: dict, en_ambito: Column) -> Column:
    """Marca **todas** las filas que comparten un valor repetido.

    No se marca "la segunda y siguientes": con la información disponible no hay
    forma de saber cuál de las copias es la buena, y elegir una arbitrariamente
    daría una falsa sensación de haber resuelto el conflicto.

    El parámetro `with` añade columnas a la clave de unicidad. Lo exigen las
    dimensiones historificadas: en una tabla SCD2 la clave de negocio aparece
    una vez por versión, así que la unicidad es de (clave, marca de tiempo).
    Comprobando solo la clave, cada fila con historial se marcaría como
    duplicada y el historial haría fallar su propia validación.

    El recuento se restringe además al ámbito de la regla, por si `where` la
    acota a un subconjunto.
    """
    adicionales = [F.col(c) for c in regla.params.get("with", [])]
    repeticiones = F.sum(F.when(en_ambito, 1).otherwise(0)).over(
        Window.partitionBy(columna, *adicionales)
    )
    return repeticiones > 1


def _foreign_key(
    columna: Column, regla: Rule, refs: dict[str, DataFrame], _ambito: Column
) -> Column:
    """Marca los valores que no existen en la tabla referenciada.

    Es la anomalía que el tipado no detecta: un identificador inexistente es
    una cadena perfectamente válida.
    """
    nombre = regla.params["ref"]
    if nombre not in refs:
        raise ValueError(
            f"La regla foreign_key de {regla.table}.{regla.column} necesita la tabla "
            f"'{nombre}', que no se ha proporcionado. Sin ella la comprobación no "
            f"puede realizarse, y darla por superada haría que el reporte mintiera."
        )

    referencia = refs[nombre]
    ref_columna = regla.params.get("ref_column", regla.column)
    valores = referencia.select(F.col(ref_columna).alias("_ref")).distinct()

    # El nulo lo cubre `not_null`; aquí solo interesan las referencias rotas.
    return columna.isNotNull() & ~columna.isin([fila["_ref"] for fila in valores.collect()])


_PREDICADOS = {
    "not_null": _not_null,
    "in_set": _in_set,
    "min_value": _min_value,
    "max_value": _max_value,
    "matches": _matches,
    "unique": _unique,
    "foreign_key": _foreign_key,
}


def _row_count_delta(df: DataFrame, regla: Rule, baselines: dict[str, int]) -> Outcome:
    """Compara el volumen de la tabla con el de la ejecución anterior.

    El resultado es binario: o la variación está dentro del umbral o no. Para
    que la tasa de aprobación del reporte siga significando algo, un
    incumplimiento marca la tabla entera —0 %— en lugar de un número de filas
    que aquí no tendría sentido: no hay una fila culpable, el problema es del
    conjunto.
    """
    actual = df.count()
    base = baselines.get(regla.table)

    def resultado(fallo: bool, detalle: str) -> Outcome:
        return Outcome(
            table=regla.table,
            column=regla.column,
            rule=regla.rule,
            severity=regla.severity,
            rows_checked=actual,
            rows_failed=actual if fallo else 0,
            detail=detalle,
        )

    # Primera ejecución: no hay con qué comparar. Marcarlo como fallo haría que
    # todo pipeline nuevo arrancara en rojo por no tener pasado.
    if base is None:
        return resultado(False, f"{actual:,} filas, sin línea base con la que comparar")

    if base == 0:
        return resultado(False, f"{actual:,} filas, la línea base era cero")

    variacion = 100.0 * (actual - base) / base
    umbral = float(regla.params.get("max_pct", 50))
    detalle = f"{base:,} -> {actual:,} filas ({variacion:+.1f}%, umbral ±{umbral:.0f}%)"
    return resultado(abs(variacion) > umbral, detalle)


def evaluate(
    df: DataFrame,
    reglas: list[Rule],
    references: dict[str, DataFrame] | None = None,
    baselines: dict[str, int] | None = None,
) -> tuple[DataFrame, list[Outcome]]:
    """Aplica las reglas y devuelve `(df marcado, resultados)`.

    El DataFrame marcado gana la columna `_rule_errors`, con las etiquetas de
    las reglas de severidad `quarantine` o `fail` que la fila incumple. Los
    avisos no entran: informan, pero no retiran datos.
    """
    refs = references or {}
    lineas_base = baselines or {}

    # Las reglas de tabla se evalúan aparte: no tienen predicado por fila, no
    # marcan ninguna fila para cuarentena y su resultado es binario.
    resultados_tabla = [
        _row_count_delta(df, regla, lineas_base) for regla in reglas if regla.is_table_level
    ]
    reglas = [regla for regla in reglas if not regla.is_table_level]

    # Cada incumplimiento se materializa primero como una columna booleana y
    # solo después se agrega. No es un rodeo: `unique` usa una función de
    # ventana, y Spark no permite ventanas dentro de una agregación. Al
    # calcularlas en la proyección, el conteo posterior opera sobre booleanos
    # corrientes y todas las reglas se tratan igual.
    marcado = df
    aplicables: list[tuple[Rule, str, str]] = []

    for indice, regla in enumerate(reglas):
        if regla.column and regla.column not in df.columns:
            continue

        # `where` acota la regla a un subconjunto. Nace de las dimensiones
        # SCD2: la clave de negocio se repite —una fila por versión— y `unique`
        # solo tiene sentido sobre las vigentes. Sin esto, historificar una
        # dimensión haría fallar su propia regla de unicidad.
        filtro = regla.params.get("where")
        en_ambito = F.expr(filtro) if filtro else F.lit(True)

        columna_ambito, columna_violacion = f"_s{indice}", f"_v{indice}"
        incumple = _PREDICADOS[regla.rule](F.col(regla.column), regla, refs, en_ambito)

        marcado = marcado.withColumn(columna_ambito, en_ambito).withColumn(
            columna_violacion, F.col(columna_ambito) & incumple
        )
        aplicables.append((regla, columna_ambito, columna_violacion))

    # Una sola pasada de agregación para todas las reglas, en lugar de un
    # `count()` por regla: con ocho tablas y decenas de reglas, la diferencia
    # entre una consulta y cuarenta es sustancial.
    agregados = (
        marcado.agg(
            *[
                expresion
                for _, ambito, violacion in aplicables
                for expresion in (
                    F.sum(F.when(F.col(ambito), 1).otherwise(0)).alias(ambito),
                    F.sum(F.when(F.col(violacion), 1).otherwise(0)).alias(violacion),
                )
            ]
        ).first()
        if aplicables
        else None
    )

    resultados = [
        Outcome(
            table=regla.table,
            column=regla.column,
            rule=regla.rule,
            severity=regla.severity,
            # Las filas comprobadas son las del ámbito, no las de la tabla: si
            # dijera 1.000 habiendo mirado 400, la tasa de aprobación del
            # reporte sería engañosa.
            rows_checked=int(agregados[ambito] or 0),
            rows_failed=int(agregados[violacion] or 0),
        )
        for regla, ambito, violacion in aplicables
    ]

    # Los avisos no entran en la marca: informan, pero no retiran datos.
    marcas: list[Column] = [
        F.when(F.col(violacion), F.lit(regla.label))
        for regla, _, violacion in aplicables
        if regla.severity in ("quarantine", "fail")
    ]
    errores = F.array_compact(F.array(*marcas)) if marcas else F.array().cast("array<string>")

    auxiliares = [c for _, ambito, violacion in aplicables for c in (ambito, violacion)]
    return (
        marcado.withColumn(RULE_ERRORS, errores).drop(*auxiliares),
        resultados_tabla + resultados,
    )


def hay_fallos_bloqueantes(resultados: list[Outcome]) -> bool:
    """¿Alguna regla de severidad `fail` fue incumplida?

    Es lo que consulta la puerta de calidad para detener el pipeline antes de
    que Gold publique cifras construidas sobre datos que no cumplen lo mínimo.
    """
    return any(r.severity == "fail" and not r.passed for r in resultados)
