#!/usr/bin/env python3
"""Muestra cómo están organizadas físicamente las tablas Gold: número de
ficheros y tamaño medio.

Es una medida de rendimiento fiable porque no depende de cachés ni de la
latencia de system.query.history. Tener muchos ficheros pequeños perjudica el
rendimiento, ya que cada fichero requiere operaciones adicionales y aumenta el
coste de coordinación de las consultas.

Uso:
    python scripts/table_layout.py
"""

from __future__ import annotations

import sys

from measure_performance import ejecutar, warehouse_id

TABLAS = (
    "ecommerce.gold.daily_sales",
    "ecommerce.gold.category_performance",
    "ecommerce.gold.marketing_funnel",
    "ecommerce.gold.product_performance",
    "ecommerce.gold.customer_lifetime_value",
    "ecommerce.gold.customer_segments",
    "ecommerce.silver.orders",
    "ecommerce.silver.web_events",
)

# Por debajo de este tamaño, el coste de abrir el fichero domina sobre el de
# leerlo. Es un umbral de sentido común, no una cifra oficial.
FICHERO_PEQUENO_KB = 128


def detalle(wid: str, tabla: str) -> dict[str, str]:
    _, respuesta = ejecutar(wid, f"DESCRIBE DETAIL {tabla}")
    columnas = [c["name"] for c in respuesta["manifest"]["schema"]["columns"]]
    return dict(zip(columnas, respuesta["result"]["data_array"][0], strict=True))


def main() -> int:
    wid = warehouse_id()

    print(f"{'tabla':<42}{'filas/fich':>12}{'ficheros':>10}{'medio':>12}{'organización':>22}")
    print("-" * 98)

    sospechosas = []
    for tabla in TABLAS:
        datos = detalle(wid, tabla)
        ficheros = int(datos["numFiles"])
        octetos = int(datos["sizeInBytes"])
        medio_kb = octetos / ficheros / 1024 if ficheros else 0

        _, filas = ejecutar(wid, f"SELECT count(*) FROM {tabla}")
        n_filas = int(filas["result"]["data_array"][0][0])

        particion = datos.get("partitionColumns") or "[]"
        cluster = datos.get("clusteringColumns") or "[]"
        organizacion = (
            f"particionada {particion}"
            if particion != "[]"
            else (f"clustering {cluster}" if cluster != "[]" else "sin organizar")
        )

        marca = "  <-- " if medio_kb < FICHERO_PEQUENO_KB and ficheros > 1 else "  "
        print(
            f"{tabla:<42}{n_filas // max(ficheros, 1):>12}{ficheros:>10}"
            f"{medio_kb:>10.1f} KB{organizacion:>22}{marca}"
        )
        if medio_kb < FICHERO_PEQUENO_KB and ficheros > 1:
            sospechosas.append((tabla, ficheros, medio_kb))

    if sospechosas:
        print(f"\nTablas con ficheros por debajo de {FICHERO_PEQUENO_KB} KB:")
        for tabla, ficheros, medio in sospechosas:
            print(f"  {tabla}: {ficheros} ficheros de {medio:.1f} KB de media")

    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(__file__.rsplit("/", 1)[0]))
    raise SystemExit(main())
