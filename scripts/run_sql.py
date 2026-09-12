#!/usr/bin/env python3
"""Ejecuta un fichero .sql contra un SQL Warehouse de Databricks.

El CLI de Databricks no sabe ejecutar ficheros .sql, así que este script cubre
ese hueco. Se usa para el bootstrap del namespace y para validar las consultas
del dashboard sin salir del contenedor.

Uso:
    python scripts/run_sql.py sql/00_bootstrap.sql
    python scripts/run_sql.py sql/kpis.sql --warehouse <id>

Credenciales: DATABRICKS_HOST y DATABRICKS_TOKEN del entorno (las inyecta
docker-compose desde .env).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# El warehouse serverless arranca en frío. La API acepta como mucho 50s de
# espera sincrónica, así que después hay que sondear.
WAIT_TIMEOUT = "50s"
POLL_INTERVAL_S = 3
POLL_MAX_S = 300


def _api(host: str, token: str, method: str, path: str, payload: dict | None = None) -> dict:
    request = urllib.request.Request(
        f"{host.rstrip('/')}{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        sys.exit(f"HTTP {error.code} en {path}: {error.read().decode()}")


def split_statements(sql: str) -> list[str]:
    """Divide el fichero en sentencias sueltas.

    La API de ejecución acepta una sentencia por llamada. La división es por
    punto y coma a nivel de línea, tras descartar comentarios `--`; es
    suficiente para DDL y consultas analíticas, que es todo lo que ejecuta este
    proyecto. No contempla puntos y coma dentro de literales de cadena.
    """
    sin_comentarios = "\n".join(
        linea for linea in sql.splitlines() if not linea.lstrip().startswith("--")
    )
    return [s.strip() for s in sin_comentarios.split(";") if s.strip()]


def first_warehouse_id(host: str, token: str) -> str:
    warehouses = _api(host, token, "GET", "/api/2.0/sql/warehouses").get("warehouses", [])
    if not warehouses:
        sys.exit("No hay ningún SQL Warehouse en el workspace.")
    return warehouses[0]["id"]


def execute(host: str, token: str, warehouse_id: str, statement: str) -> dict:
    """Lanza una sentencia y espera a que termine, sondeando si hace falta."""
    response = _api(
        host,
        token,
        "POST",
        "/api/2.0/sql/statements",
        {"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": WAIT_TIMEOUT},
    )

    esperando = 0
    while response["status"]["state"] in ("PENDING", "RUNNING"):
        if esperando >= POLL_MAX_S:
            sys.exit(f"La sentencia no terminó en {POLL_MAX_S}s: {statement[:60]}...")
        time.sleep(POLL_INTERVAL_S)
        esperando += POLL_INTERVAL_S
        response = _api(host, token, "GET", f"/api/2.0/sql/statements/{response['statement_id']}")

    return response


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fichero", help="Ruta al fichero .sql")
    parser.add_argument("--warehouse", help="ID del warehouse (por defecto, el primero)")
    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        sys.exit("Faltan DATABRICKS_HOST y/o DATABRICKS_TOKEN. ¿Existe el fichero .env?")

    statements = split_statements(Path(args.fichero).read_text(encoding="utf-8"))
    warehouse_id = args.warehouse or first_warehouse_id(host, token)
    print(f"Warehouse {warehouse_id} · {len(statements)} sentencia(s) de {args.fichero}\n")

    for numero, statement in enumerate(statements, start=1):
        resumen = " ".join(statement.split())[:70]
        response = execute(host, token, warehouse_id, statement)
        estado = response["status"]["state"]

        if estado != "SUCCEEDED":
            mensaje = response["status"].get("error", {}).get("message", "sin detalle")
            print(f"  [{numero}/{len(statements)}] FALLO  {resumen}\n         {mensaje}")
            return 1

        filas = response.get("result", {}).get("data_array")
        print(f"  [{numero}/{len(statements)}] OK     {resumen}")
        if filas:
            columnas = [c["name"] for c in response["manifest"]["schema"]["columns"]]
            print(f"         {columnas}")
            for fila in filas[:20]:
                print(f"         {fila}")

    print("\nCompletado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
