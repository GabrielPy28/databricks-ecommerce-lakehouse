#!/usr/bin/env python3
"""El script mide el efecto real de las optimizaciones de Delta centrándose en
**bytes y ficheros leídos**, ya que en Free Edition los tiempos de ejecución
dependen demasiado del entorno serverless.

Para evitar resultados engañosos, desactiva el efecto de las cachés mediante un
comentario único para evitar la caché de resultados y reiniciando el warehouse
antes de cada fase para vaciar la caché de disco. Además, compara el _layout_ de
las tablas antes y después de `OPTIMIZE` mediante `DESCRIBE DETAIL` (número de
ficheros y tamaño medio), y espera a que `system.query.history` registre las
consultas debido a su actualización asíncrona.

Uso:
    python scripts/measure_performance.py
    python scripts/measure_performance.py --solo-medir   # sin optimizar
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass

ESPERA_HISTORIAL_S = 900
INTERVALO_S = 20

# Parar el warehouse drena primero las sesiones abiertas; tras un OPTIMIZE
# puede tardar bastante más que arrancarlo.
ESPERA_PARADA_S = 1_200
ESPERA_ARRANQUE_S = 600

# Consultas representativas del uso real del dashboard y del análisis.
CONSULTAS: dict[str, str] = {
    "daily_sales_total": """
        SELECT sum(net_revenue) AS ingreso, count(*) AS dias
        FROM ecommerce.gold.daily_sales
    """,
    "daily_sales_ventana": """
        SELECT sum(net_revenue) AS ingreso
        FROM ecommerce.gold.daily_sales
        WHERE date BETWEEN '2026-06-01' AND '2026-08-31'
    """,
    "orders_por_rango": """
        SELECT count(*) AS ordenes, sum(total_amount) AS importe
        FROM ecommerce.silver.orders
        WHERE order_date >= '2026-06-01' AND order_date < '2026-09-01'
    """,
    "web_events_embudo": """
        SELECT event_type, count(DISTINCT session_id) AS sesiones
        FROM ecommerce.silver.web_events
        GROUP BY event_type
    """,
}

# Optimizaciones a aplicar entre ambas mediciones.
#
# `daily_sales` está particionada por día: 698 días producen 698 ficheros
# diminutos, y leer la tabla entera abre uno por partición. La compactación es
# el caso de libro.
#
# `silver.orders` y `silver.web_events` no están particionadas, así que lo que
# ayuda es agrupar por la columna de filtro para que el salto de datos pueda
# descartar ficheros enteros.
OPTIMIZACIONES = (
    "OPTIMIZE ecommerce.gold.daily_sales",
    "OPTIMIZE ecommerce.silver.orders ZORDER BY (order_date)",
    "OPTIMIZE ecommerce.silver.web_events ZORDER BY (event_timestamp)",
)

TABLA_RESULTADOS = "ecommerce.ops.performance_results"


@dataclass
class Medicion:
    consulta: str
    fase: str
    statement_id: str
    read_bytes: int = 0
    read_files: int = 0
    duration_ms: int = 0


def _api(method: str, path: str, payload: dict | None = None) -> dict:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    peticion = urllib.request.Request(
        f"{host}{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(peticion) as respuesta:
            return json.loads(respuesta.read())
    except urllib.error.HTTPError as error:
        sys.exit(f"HTTP {error.code} en {path}: {error.read().decode()[:400]}")


def warehouse_id() -> str:
    almacenes = _api("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
    if not almacenes:
        sys.exit("No hay ningún SQL Warehouse.")
    return almacenes[0]["id"]


def ejecutar(wid: str, sql: str) -> tuple[str, dict]:
    """Lanza una sentencia y espera a que termine. Devuelve (statement_id, respuesta)."""
    respuesta = _api(
        "POST",
        "/api/2.0/sql/statements",
        {"warehouse_id": wid, "statement": sql, "wait_timeout": "50s"},
    )
    statement_id = respuesta["statement_id"]

    esperando = 0
    while respuesta["status"]["state"] in ("PENDING", "RUNNING"):
        if esperando > 600:
            sys.exit(f"La sentencia {statement_id} no terminó.")
        time.sleep(5)
        esperando += 5
        respuesta = _api("GET", f"/api/2.0/sql/statements/{statement_id}")

    if respuesta["status"]["state"] != "SUCCEEDED":
        detalle = respuesta["status"].get("error", {}).get("message", "sin detalle")
        sys.exit(f"Falló: {detalle}")

    return statement_id, respuesta


TABLAS_MEDIDAS = (
    "ecommerce.gold.daily_sales",
    "ecommerce.silver.orders",
    "ecommerce.silver.web_events",
)


def layout(wid: str) -> dict[str, tuple[int, int]]:
    """Ficheros y bytes de cada tabla, según `DESCRIBE DETAIL`.

    Independiente de cualquier caché: describe cómo están escritos los datos,
    no cómo se leyeron. Es la evidencia más difícil de falsear del efecto de
    `OPTIMIZE`.
    """
    resultado = {}
    for tabla in TABLAS_MEDIDAS:
        _, respuesta = ejecutar(wid, f"DESCRIBE DETAIL {tabla}")
        columnas = [c["name"] for c in respuesta["manifest"]["schema"]["columns"]]
        fila = respuesta["result"]["data_array"][0]
        datos = dict(zip(columnas, fila, strict=True))
        resultado[tabla] = (int(datos["numFiles"]), int(datos["sizeInBytes"]))
    return resultado


def enfriar_cache(wid: str) -> None:
    """Reinicia el warehouse para vaciar su caché de disco.

    Sin esto, una consulta que lee ficheros recién escritos por `OPTIMIZE` los
    encuentra en la caché local y reporta cero bytes leídos: la medición diría
    que la optimización fue perfecta cuando en realidad no se midió nada.
    """
    print("  reiniciando el warehouse para vaciar la caché de disco...")

    def esperar(objetivo: str, limite: int) -> None:
        esperando = 0
        anterior = None
        while True:
            estado = _api("GET", f"/api/2.0/sql/warehouses/{wid}")["state"]
            if estado == objetivo:
                return
            if estado != anterior:
                print(f"    {estado}...")
                anterior = estado
            if esperando > limite:
                sys.exit(
                    f"El warehouse no alcanzó {objetivo} en {limite}s (está en {estado}). "
                    f"Sin caché fría la medición no sería comparable, así que se detiene "
                    f"en lugar de publicar cifras que medirían la caché."
                )
            time.sleep(15)
            esperando += 15

    # Parar un warehouse tarda más de lo que parece: primero drena las sesiones
    # abiertas y solo después pasa a STOPPED. Tras un OPTIMIZE largo puede
    # superar holgadamente los diez minutos.
    _api("POST", f"/api/2.0/sql/warehouses/{wid}/stop", {})
    esperar("STOPPED", ESPERA_PARADA_S)

    _api("POST", f"/api/2.0/sql/warehouses/{wid}/start", {})
    esperar("RUNNING", ESPERA_ARRANQUE_S)
    print("  caché vacía.")


def medir_fase(wid: str, fase: str) -> list[Medicion]:
    """Ejecuta todas las consultas de referencia y devuelve sus identificadores."""
    mediciones = []
    for nombre, sql in CONSULTAS.items():
        # El comentario único evita la caché de resultados sin tocar el plan.
        marca = uuid.uuid4().hex[:12]
        statement_id, _ = ejecutar(wid, f"-- benchmark {fase} {nombre} {marca}\n{sql}")
        mediciones.append(Medicion(consulta=nombre, fase=fase, statement_id=statement_id))
        print(f"  {fase:<8} {nombre:<22} {statement_id}")
    return mediciones


def recuperar_metricas(wid: str, mediciones: list[Medicion]) -> None:
    """Completa las mediciones desde `system.query.history`, esperando su latencia."""
    pendientes = {m.statement_id: m for m in mediciones}
    ids = ", ".join(f"'{i}'" for i in pendientes)

    print(f"\nEsperando a que {len(pendientes)} consultas aparezcan en system.query.history...")
    esperando = 0
    while pendientes:
        _, respuesta = ejecutar(
            wid,
            f"""SELECT statement_id, read_bytes, read_files, total_duration_ms
                FROM system.query.history
                WHERE statement_id IN ({ids})""",
        )
        for fila in respuesta.get("result", {}).get("data_array", []) or []:
            statement_id, leidos, ficheros, duracion = fila
            medicion = pendientes.pop(statement_id, None)
            if medicion:
                medicion.read_bytes = int(leidos or 0)
                medicion.read_files = int(ficheros or 0)
                medicion.duration_ms = int(duracion or 0)

        if not pendientes:
            break
        if esperando >= ESPERA_HISTORIAL_S:
            print(f"  {len(pendientes)} consultas siguen sin aparecer tras {esperando}s.")
            break
        time.sleep(INTERVALO_S)
        esperando += INTERVALO_S
        print(f"  ...{esperando}s, faltan {len(pendientes)}")


def _porcentaje(antes: int, despues: int) -> str:
    if antes == 0:
        return "n/d"
    return f"{100 * (antes - despues) / antes:+.1f}%"


def informe_layout(antes: dict[str, tuple[int, int]], despues: dict[str, tuple[int, int]]) -> None:
    print(f"\n{'tabla':<32}{'ficheros':>22}{'tamaño medio':>26}")
    print("-" * 80)
    for tabla, (ficheros_a, bytes_a) in antes.items():
        ficheros_d, bytes_d = despues[tabla]
        medio_a = bytes_a / ficheros_a / 1024 if ficheros_a else 0
        medio_d = bytes_d / ficheros_d / 1024 if ficheros_d else 0
        cambio = f"{ficheros_a} -> {ficheros_d} ({_porcentaje(ficheros_a, ficheros_d)})"
        tamano = f"{medio_a:.0f} -> {medio_d:.0f} KB"
        print(f"{tabla:<32}{cambio:>22}{tamano:>26}")


def informe(antes: list[Medicion], despues: list[Medicion]) -> None:
    por_consulta = {m.consulta: m for m in despues}

    print(f"\n{'consulta':<24}{'ficheros':>20}{'bytes leídos':>26}{'duración':>20}")
    print("-" * 90)

    for a in antes:
        d = por_consulta[a.consulta]
        ficheros = f"{a.read_files} -> {d.read_files} ({_porcentaje(a.read_files, d.read_files)})"
        octetos = (
            f"{a.read_bytes / 1e6:.2f} -> {d.read_bytes / 1e6:.2f} MB "
            f"({_porcentaje(a.read_bytes, d.read_bytes)})"
        )
        tiempo = f"{a.duration_ms} -> {d.duration_ms} ms"
        print(f"{a.consulta:<24}{ficheros:>20}{octetos:>26}{tiempo:>20}")

    print(
        "\nLos bytes y ficheros son la medida fiable: el tiempo en serverless "
        "depende del estado del almacén y se incluye solo como referencia."
    )


def persistir(wid: str, mediciones: list[Medicion]) -> None:
    """Guarda las mediciones para que el README cite números con respaldo."""
    ejecutar(
        wid,
        f"""CREATE TABLE IF NOT EXISTS {TABLA_RESULTADOS} (
                measured_at   TIMESTAMP,
                query         STRING,
                phase         STRING,
                statement_id  STRING,
                read_bytes    BIGINT,
                read_files    BIGINT,
                duration_ms   BIGINT
            ) USING DELTA""",
    )
    valores = ", ".join(
        f"(current_timestamp(), '{m.consulta}', '{m.fase}', '{m.statement_id}', "
        f"{m.read_bytes}, {m.read_files}, {m.duration_ms})"
        for m in mediciones
    )
    ejecutar(wid, f"INSERT INTO {TABLA_RESULTADOS} VALUES {valores}")
    print(f"\nMediciones guardadas en {TABLA_RESULTADOS}.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--solo-medir",
        action="store_true",
        help="Mide sin aplicar las optimizaciones (para repetir una medición).",
    )
    args = parser.parse_args()

    wid = warehouse_id()
    print(f"Warehouse {wid}\n")

    print("Midiendo ANTES de optimizar:")
    enfriar_cache(wid)
    layout_antes = layout(wid)
    antes = medir_fase(wid, "antes")

    if not args.solo_medir:
        print("\nAplicando optimizaciones:")
        for sentencia in OPTIMIZACIONES:
            ejecutar(wid, sentencia)
            print(f"  {sentencia}")

    print("\nMidiendo DESPUÉS:")
    # El reinicio va DESPUÉS de optimizar: `OPTIMIZE` deja calientes los
    # ficheros que acaba de escribir, y sin vaciar la caché la medición leería
    # cero bytes y declararía una mejora del 100 % que no existe.
    enfriar_cache(wid)
    layout_despues = layout(wid)
    despues = medir_fase(wid, "despues")

    recuperar_metricas(wid, antes + despues)
    informe_layout(layout_antes, layout_despues)
    informe(antes, despues)
    persistir(wid, antes + despues)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
