#!/usr/bin/env python3
"""Resume una ejecución del job: resultado y duración de cada tarea.

`databricks bundle run` informa del resultado global, pero al medir el
rendimiento del pipeline interesa saber qué tarea se lleva el tiempo. Esto lo
imprime en una tabla legible a partir de la respuesta del API.

Uso:
    python scripts/run_summary.py                 # la última ejecución
    python scripts/run_summary.py <run_id>
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request


def _api(path: str) -> dict:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    peticion = urllib.request.Request(f"{host}{path}", headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(peticion) as respuesta:
            return json.loads(respuesta.read())
    except urllib.error.HTTPError as error:
        sys.exit(f"HTTP {error.code}: {error.read().decode()[:300]}")


def main() -> int:
    if len(sys.argv) > 1:
        run_id = sys.argv[1]
    else:
        runs = _api("/api/2.1/jobs/runs/list?limit=1").get("runs", [])
        if not runs:
            sys.exit("No hay ejecuciones.")
        run_id = runs[0]["run_id"]

    detalle = _api(f"/api/2.1/jobs/runs/get?run_id={run_id}")
    estado = detalle["state"]

    print(f"Ejecución {run_id}")
    print(f"  estado    {estado.get('result_state', estado.get('life_cycle_state'))}")
    print(f"  duración  {detalle.get('run_duration', 0) // 1000}s")
    print(f"  {detalle.get('run_page_url', '')}")
    print()
    print(f"  {'tarea':<18}{'resultado':<14}{'duración':>10}")
    print(f"  {'-' * 42}")

    ahora = int(time.time() * 1000)

    for tarea in sorted(detalle.get("tasks", []), key=lambda t: t.get("start_time", 0)):
        resultado = tarea["state"].get("result_state", tarea["state"].get("life_cycle_state", ""))

        # `execution_duration` solo se rellena al terminar la tarea. Para una en
        # curso se calcula desde su hora de inicio: sin esto no hay forma de
        # saber si una tarea avanza o está atascada.
        if tarea.get("execution_duration"):
            segundos = tarea["execution_duration"] // 1000
            marca = ""
        elif tarea.get("start_time"):
            segundos = (ahora - tarea["start_time"]) // 1000
            marca = " (en curso)"
        else:
            segundos, marca = 0, ""

        print(f"  {tarea['task_key']:<18}{resultado:<14}{segundos:>9}s{marca}")

    return 0 if estado.get("result_state") == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
