"""Manifiesto de lote.

Acompaña a cada lote y registra todo lo necesario para reproducirlo y para
explicar sus anomalías. Es lo que permite que el reporte de calidad diga
"2.314 registros en cuarentena" y se pueda justificar de dónde salió cada uno.

Se escribe en `_manifests/`, **fuera** de los directorios de entidad: Auto
Loader se suscribe a `<entidad>/` y espera un esquema homogéneo, así que un
JSON dentro de ese árbol rompería la inferencia de esquema.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MANIFESTS_DIR = "_manifests"

# Cambiar la lógica de generación sin cambiar esto haría que dos lotes con la
# misma semilla y distinto contenido fueran indistinguibles.
GENERATOR_VERSION = "1.0.0"


def build(
    *,
    seed: int,
    profile: str,
    batch: int,
    chunk_orders: int,
    rates: dict[str, float],
    rows: dict[str, int],
    injected: dict[str, dict[str, int]],
    window: dict[str, str],
    incremental: dict[str, int] | None = None,
) -> dict[str, Any]:
    documento: dict[str, Any] = {
        "generator_version": GENERATOR_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "seed": seed,
        "profile": profile,
        "batch": batch,
        # El troceado forma parte de la reproducibilidad: con la misma semilla
        # pero distinto tamaño de trozo, las tiradas del generador se reparten
        # de otra manera y el detalle de las líneas cambia.
        "chunk_orders": chunk_orders,
        "window": window,
        "rates": rates,
        "rows": rows,
        "injected": injected,
    }

    # Solo en los lotes incrementales. Sin este desglose, el número de filas de
    # un lote no se puede explicar: no se sabe cuántas son órdenes nuevas,
    # cuántas mutaciones de órdenes anteriores y cuántas llegaron tarde.
    if incremental is not None:
        documento["incremental"] = incremental

    return documento


def write(manifiesto: dict[str, Any], output_root: Path, batch_label: str) -> Path:
    destino = output_root / MANIFESTS_DIR
    destino.mkdir(parents=True, exist_ok=True)
    ruta = destino / f"{batch_label}.json"
    ruta.write_text(json.dumps(manifiesto, indent=2, ensure_ascii=False), encoding="utf-8")
    return ruta
