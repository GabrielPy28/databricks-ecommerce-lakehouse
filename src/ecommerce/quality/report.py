"""Reporte de calidad legible y filas para `ops.quality_results`.

Dos salidas de los mismos resultados:

* **El reporte**, para leer en la ejecución del job.
* **Las filas**, para `ops.quality_results`, que conserva el histórico y
  permite graficar la evolución de la calidad en el tiempo. Un reporte que
  solo se imprime se pierde en cuanto se cierra el log.
"""

from __future__ import annotations

from typing import Any

from ecommerce.quality.engine import Outcome

MARCA_OK = "✓"
MARCA_FALLO = "✗"
ANCHO = 60


def format_report(resultados: list[Outcome]) -> str:
    """Reporte legible, agrupado por tabla."""
    lineas = ["Data Quality Report", "─" * ANCHO]

    por_tabla: dict[str, list[Outcome]] = {}
    for resultado in resultados:
        por_tabla.setdefault(resultado.table, []).append(resultado)

    for tabla, salidas in por_tabla.items():
        procesadas = salidas[0].rows_checked
        lineas += ["", tabla, "", f"  Registros procesados: {procesadas:>12,}", ""]

        for salida in salidas:
            marca = MARCA_OK if salida.passed else MARCA_FALLO
            etiqueta = f"{salida.column} {salida.rule}".strip()
            lineas.append(f"  {marca} {etiqueta:<38} {salida.pass_rate:>7.2f}%")
            # El detalle solo aparece cuando aporta algo: una regla por fila se
            # explica con su porcentaje, pero "row_count_delta falló" sin decir
            # de cuántas filas a cuántas no permite actuar.
            if salida.detail:
                lineas.append(f"      {salida.detail}")

        # Filas apartadas: las que incumplen alguna regla que retira datos.
        # Se cuentan reglas incumplidas, no filas distintas —una fila puede
        # incumplir varias—, así que es una cota superior.
        apartadas = sum(s.rows_failed for s in salidas if s.severity in ("quarantine", "fail"))
        avisos = sum(s.rows_failed for s in salidas if s.severity == "warn")

        lineas += ["", f"  Cuarentena (reglas incumplidas): {apartadas:>8,}"]
        if avisos:
            lineas.append(f"  Avisos, no apartan:              {avisos:>8,}")

    return "\n".join(lineas)


def to_rows(resultados: list[Outcome], *, run_id: str, checked_at: str) -> list[dict[str, Any]]:
    """Convierte los resultados en filas para `ops.quality_results`."""
    return [
        {
            "run_id": run_id,
            "checked_at": checked_at,
            "table": r.table,
            "column": r.column,
            "rule": r.rule,
            "severity": r.severity,
            "rows_checked": r.rows_checked,
            "rows_failed": r.rows_failed,
            "pass_rate": round(r.pass_rate, 4),
            "detail": r.detail,
        }
        for r in resultados
    ]
