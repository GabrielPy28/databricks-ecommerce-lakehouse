#!/usr/bin/env python3
"""Genera `ecommerce_overview.lvdash.json` desde una declaración compacta.

Aquí se declara lo que un quiere visualizar, es decir —qué consulta, qué columnas, qué
título— y el expansor añade el resto. El JSON generado se commitea porque es lo
que el Asset Bundle despliega; un test comprueba que no se desincronice de esta
declaración.

Uso:
    python dashboards/build_dashboard.py          # escribe el .lvdash.json
    python dashboards/build_dashboard.py --check  # falla si está desactualizado
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

DESTINO = Path(__file__).parent / "ecommerce_overview.lvdash.json"

# Tipos de columna de tabla.
TIPOS = {
    "text": ("string", "string", "left"),
    "int": ("integer", "number", "right"),
    "num": ("float", "number", "right"),
}


# --------------------------------------------------------------------------
# Declaración
# --------------------------------------------------------------------------

CATALOGO = "ecommerce"

DATASETS: dict[str, tuple[str, str]] = {
    "ds_kpis": (
        "KPIs de cabecera",
        f"""SELECT
    sum(net_revenue)                   AS ingreso_neto,
    sum(orders)                        AS ordenes,
    round(avg(average_order_value), 2) AS ticket_medio,
    (SELECT count(*) FROM {CATALOGO}.gold.customer_lifetime_value) AS clientes
FROM {CATALOGO}.gold.daily_sales""",
    ),
    "ds_daily": (
        "Ingreso diario",
        f"""SELECT date, net_revenue, orders, customers
FROM {CATALOGO}.gold.daily_sales
ORDER BY date""",
    ),
    "ds_country": (
        "Ingreso por país",
        f"""SELECT country, sum(total_spend) AS ingreso, count(*) AS clientes
FROM {CATALOGO}.gold.customer_lifetime_value
WHERE country IS NOT NULL
GROUP BY country
ORDER BY ingreso DESC""",
    ),
    "ds_segments": (
        "Segmentos RFM",
        f"""SELECT segment, count(*) AS clientes, round(sum(total_spend), 2) AS ingreso
FROM {CATALOGO}.gold.customer_segments
GROUP BY segment
ORDER BY ingreso DESC""",
    ),
    "ds_products": (
        "Productos por ingreso",
        f"""SELECT product_name, category, units_sold, revenue, profit,
       round(margin_pct, 1) AS margen_pct, round(return_rate, 1) AS devolucion_pct
FROM {CATALOGO}.gold.product_performance
ORDER BY revenue DESC
LIMIT 20""",
    ),
    "ds_funnel": (
        "Embudo por canal",
        f"""SELECT channel,
       sum(page_views)   AS visitas,
       sum(add_to_carts) AS al_carrito,
       sum(checkouts)    AS inicio_pago,
       sum(purchases)    AS compras,
       round(100.0 * sum(purchases) / nullif(sum(page_views), 0), 2) AS conversion_pct
FROM {CATALOGO}.gold.marketing_funnel
GROUP BY channel
ORDER BY visitas DESC""",
    ),
    "ds_quality": (
        "Calidad de la última ejecución",
        f"""WITH ultima AS (
    SELECT run_id FROM {CATALOGO}.ops.quality_results
    ORDER BY checked_at DESC LIMIT 1
)
SELECT table AS tabla, column AS columna, rule AS regla, severity AS severidad,
       rows_checked AS filas, rows_failed AS incumplen, round(pass_rate, 2) AS pct_ok
FROM {CATALOGO}.ops.quality_results
WHERE run_id = (SELECT run_id FROM ultima) AND rows_failed > 0
ORDER BY rows_failed DESC""",
    ),
}

INTRO = (
    "# E-Commerce Lakehouse\n\n"
    "Arquitectura Medallion sobre Databricks. Todas las cifras provienen de las "
    "tablas `gold`, reconstruidas por el pipeline orquestado `ecommerce-lakehouse`."
)

# (tipo, nombre, posición, configuración)
WIDGETS: list[dict[str, Any]] = [
    {"kind": "text", "name": "w_intro", "pos": (0, 0, 6, 2), "text": INTRO},
    {
        "kind": "counter",
        "name": "w_revenue",
        "pos": (0, 2, 2, 3),
        "dataset": "ds_kpis",
        "field": "ingreso_neto",
        "title": "Ingreso neto",
        "description": "Suma de ventas con ingreso reconocido",
    },
    {
        "kind": "counter",
        "name": "w_orders",
        "pos": (2, 2, 2, 3),
        "dataset": "ds_kpis",
        "field": "ordenes",
        "title": "Órdenes",
        "description": "Órdenes pagadas, enviadas o entregadas",
    },
    {
        "kind": "counter",
        "name": "w_customers",
        "pos": (4, 2, 2, 3),
        "dataset": "ds_kpis",
        "field": "clientes",
        "title": "Clientes con compras",
        "description": "Clientes con al menos una orden válida",
    },
    {
        "kind": "line",
        "name": "w_daily_line",
        "pos": (0, 5, 6, 7),
        "dataset": "ds_daily",
        "x": ("date", "Fecha"),
        "y": ("net_revenue", "Ingreso neto"),
        "title": "Ingreso neto por día",
    },
    {
        "kind": "bar",
        "name": "w_country_bar",
        "pos": (0, 12, 3, 7),
        "dataset": "ds_country",
        "x": ("ingreso", "Ingreso"),
        "y": ("country", "País"),
        "title": "Ingreso por país",
    },
    {
        "kind": "bar",
        "name": "w_segments_bar",
        "pos": (3, 12, 3, 7),
        "dataset": "ds_segments",
        "x": ("clientes", "Clientes"),
        "y": ("segment", "Segmento RFM"),
        "title": "Base de clientes por segmento",
    },
    {
        "kind": "table",
        "name": "w_products_table",
        "pos": (0, 19, 6, 8),
        "dataset": "ds_products",
        "title": "Top 20 productos por ingreso",
        "description": "Coste valorado al vigente en la fecha de cada venta",
        "columns": [
            ("product_name", "Producto", "text"),
            ("category", "Categoría", "text"),
            ("units_sold", "Unidades", "int"),
            ("revenue", "Ingreso", "num"),
            ("profit", "Margen", "num"),
            ("margen_pct", "% margen", "num"),
            ("devolucion_pct", "% devolución", "num"),
        ],
    },
    {
        "kind": "table",
        "name": "w_funnel_table",
        "pos": (0, 27, 6, 7),
        "dataset": "ds_funnel",
        "title": "Embudo por canal de adquisición",
        "description": "Sesiones, no eventos: tres vistas de la misma visita son una visita",
        "columns": [
            ("channel", "Canal", "text"),
            ("visitas", "Visitas", "int"),
            ("al_carrito", "Al carrito", "int"),
            ("inicio_pago", "Inició pago", "int"),
            ("compras", "Compras", "int"),
            ("conversion_pct", "% conversión", "num"),
        ],
    },
    {
        "kind": "table",
        "name": "w_quality_table",
        "pos": (0, 34, 6, 7),
        "dataset": "ds_quality",
        "title": "Calidad de datos: reglas incumplidas en la última ejecución",
        "description": "Vacío significa que ninguna regla se incumplió",
        "columns": [
            ("tabla", "Tabla", "text"),
            ("columna", "Columna", "text"),
            ("regla", "Regla", "text"),
            ("severidad", "Severidad", "text"),
            ("filas", "Filas", "int"),
            ("incumplen", "Incumplen", "int"),
            ("pct_ok", "% OK", "num"),
        ],
    },
]


# --------------------------------------------------------------------------
# Expansión al formato de Databricks
# --------------------------------------------------------------------------


def _marco(titulo: str, descripcion: str | None) -> dict[str, Any]:
    """Cabecera del widget.

    Sin `frame`, un contador aparece como un número suelto sin ninguna
    indicación de qué mide. Es el fallo que tuvo la primera versión.
    """
    return {
        "title": titulo,
        "showTitle": True,
        "description": descripcion or "",
        "showDescription": bool(descripcion),
    }


def _campo(nombre: str) -> dict[str, str]:
    return {"name": nombre, "expression": f"`{nombre}`"}


def _consulta(dataset: str, campos: list[str]) -> list[dict[str, Any]]:
    return [
        {
            "name": "main_query",
            "query": {
                "datasetName": dataset,
                "fields": [_campo(c) for c in campos],
                "disaggregated": True,
            },
        }
    ]


def _columna_tabla(indice: int, campo: str, titulo: str, clase: str) -> dict[str, Any]:
    """Columna de tabla en el formato completo que Databricks exige.

    Las plantillas de imagen y enlace no se usan, pero deben estar: omitirlas
    invalida la definición del widget entero.
    """
    tipo, mostrar_como, alineacion = TIPOS[clase]
    return {
        "fieldName": campo,
        "displayName": titulo,
        "title": titulo,
        "type": tipo,
        "displayAs": mostrar_como,
        "alignContent": alineacion,
        "order": indice,
        "visible": True,
        "allowSearch": False,
        "allowHTML": False,
        "highlightLinks": False,
        "useMonospaceFont": False,
        "preserveWhitespace": False,
        "booleanValues": ["false", "true"],
        "imageUrlTemplate": "{{ @ }}",
        "imageTitleTemplate": "{{ @ }}",
        "imageWidth": "",
        "imageHeight": "",
        "linkUrlTemplate": "{{ @ }}",
        "linkTextTemplate": "{{ @ }}",
        "linkTitleTemplate": "{{ @ }}",
        "linkOpenInNewTab": True,
        "numberFormat": "",
    }


def _widget(declaracion: dict[str, Any]) -> dict[str, Any]:
    clase = declaracion["kind"]
    x, y, ancho, alto = declaracion["pos"]
    posicion = {"x": x, "y": y, "width": ancho, "height": alto}

    if clase == "text":
        return {
            "widget": {"name": declaracion["name"], "textbox_spec": declaracion["text"]},
            "position": posicion,
        }

    marco = _marco(declaracion["title"], declaracion.get("description"))

    if clase == "counter":
        spec = {
            "version": 2,
            "widgetType": "counter",
            "encodings": {
                "value": {
                    "fieldName": declaracion["field"],
                    "displayName": declaracion["title"],
                }
            },
            "frame": marco,
        }
        campos = [declaracion["field"]]

    elif clase in ("line", "bar"):
        campo_x, titulo_x = declaracion["x"]
        campo_y, titulo_y = declaracion["y"]
        # En una barra horizontal el eje de categorías es el vertical.
        escala_x = "quantitative" if clase == "bar" else "temporal"
        escala_y = "categorical" if clase == "bar" else "quantitative"
        spec = {
            "version": 3,
            "widgetType": clase,
            "encodings": {
                "x": {
                    "fieldName": campo_x,
                    "scale": {"type": escala_x},
                    "displayName": titulo_x,
                },
                "y": {
                    "fieldName": campo_y,
                    "scale": {"type": escala_y},
                    "displayName": titulo_y,
                },
            },
            "frame": marco,
        }
        campos = [campo_x, campo_y]

    elif clase == "table":
        columnas = declaracion["columns"]
        spec = {
            "version": 1,
            "widgetType": "table",
            "encodings": {
                "columns": [
                    _columna_tabla(i, campo, titulo, tipo)
                    for i, (campo, titulo, tipo) in enumerate(columnas)
                ]
            },
            "frame": marco,
            "allowHTMLByDefault": False,
            "condensed": True,
            "invisibleColumns": [],
            "itemsPerPage": 25,
            "paginationSize": "default",
            "withRowNumber": False,
        }
        campos = [campo for campo, _, _ in columnas]

    else:  # pragma: no cover - la declaración es del repositorio
        raise ValueError(f"Tipo de widget desconocido: {clase}")

    return {
        "widget": {
            "name": declaracion["name"],
            "queries": _consulta(declaracion["dataset"], campos),
            "spec": spec,
        },
        "position": posicion,
    }


def construir() -> dict[str, Any]:
    return {
        "datasets": [
            {
                "name": nombre,
                "displayName": titulo,
                # Databricks guarda la consulta como lista de líneas con sus
                # saltos incluidos.
                "queryLines": [f"{linea}\n" for linea in sql.splitlines()],
            }
            for nombre, (titulo, sql) in DATASETS.items()
        ],
        "pages": [
            {
                "name": "pg_overview",
                "displayName": "E-Commerce Overview",
                "layout": [_widget(d) for d in WIDGETS],
            }
        ],
    }


def main() -> int:
    contenido = json.dumps(construir(), indent=2, ensure_ascii=False) + "\n"

    if "--check" in sys.argv:
        actual = DESTINO.read_text(encoding="utf-8") if DESTINO.exists() else ""
        if actual != contenido:
            print(f"{DESTINO.name} está desactualizado. Ejecuta: python {Path(__file__).name}")
            return 1
        print(f"{DESTINO.name} al día.")
        return 0

    DESTINO.write_text(contenido, encoding="utf-8")
    print(f"{DESTINO.name} generado: {len(DATASETS)} datasets, {len(WIDGETS)} widgets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
