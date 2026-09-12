#!/usr/bin/env python3
"""Deshace el último `OPTIMIZE` de las tablas del benchmark.

Existe para poder **repetir la medición de rendimiento desde el mismo punto de
partida**. Sin esto, un benchmark solo se puede ejecutar una vez: en cuanto se
compactan los ficheros, el estado "antes" desaparece y cualquier repetición
compararía dos estados ya optimizados.

Usa time travel de Delta, que es la misma capacidad que demuestra
`notebooks/99_time_travel_demo.py`, aplicada aquí a una necesidad real.

`RESTORE` no borra el historial: añade una versión nueva cuyo contenido es el de
la anterior, así que el `OPTIMIZE` deshecho sigue siendo auditable.

Uso:
    python scripts/undo_optimize.py
"""

from __future__ import annotations

import sys

from measure_performance import TABLAS_MEDIDAS, ejecutar, warehouse_id


def version_previa_al_optimize(wid: str, tabla: str) -> int | None:
    """Versión inmediatamente anterior al último OPTIMIZE de la tabla."""
    _, respuesta = ejecutar(
        wid,
        f"""SELECT version, operation
            FROM (DESCRIBE HISTORY {tabla})
            WHERE operation = 'OPTIMIZE'
            ORDER BY version DESC
            LIMIT 1""",
    )
    filas = respuesta.get("result", {}).get("data_array") or []
    if not filas:
        return None
    return int(filas[0][0]) - 1


def main() -> int:
    wid = warehouse_id()
    print(f"Warehouse {wid}\n")

    deshechas = 0
    for tabla in TABLAS_MEDIDAS:
        version = version_previa_al_optimize(wid, tabla)
        if version is None:
            print(f"  {tabla:<32} sin OPTIMIZE en el historial, no se toca")
            continue

        ejecutar(wid, f"RESTORE TABLE {tabla} TO VERSION AS OF {version}")
        _, detalle = ejecutar(wid, f"DESCRIBE DETAIL {tabla}")
        columnas = [c["name"] for c in detalle["manifest"]["schema"]["columns"]]
        datos = dict(zip(columnas, detalle["result"]["data_array"][0], strict=True))

        print(f"  {tabla:<32} restaurada a v{version}, {int(datos['numFiles']):>6} ficheros")
        deshechas += 1

    print(f"\n{deshechas} tabla(s) devueltas a su estado previo al OPTIMIZE.")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(__file__.rsplit("/", 1)[0]))
    raise SystemExit(main())
