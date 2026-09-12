"""El Asset Bundle: estructura del DAG.

`databricks bundle validate` comprueba la sintaxis, pero necesita credenciales
del workspace —resuelve el usuario para prefijar los recursos en modo
desarrollo—, así que no puede correr en CI sin un secreto.

Estos tests cubren lo que de verdad se rompe al editar el repositorio y que la
validación remota tampoco detectaría: un notebook renombrado, una dependencia
que apunta a una tarea inexistente o un parámetro que nadie define. Corren sin
credenciales y en milisegundos.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

RAIZ = Path(__file__).resolve().parents[1]
BUNDLE = RAIZ / "databricks.yml"

# El DAG que el proyecto promete: ingesta, transformación, puerta y publicación.
DAG_ESPERADO = {
    "ingest_bronze": set(),
    "build_silver": {"ingest_bronze"},
    "quality_gate": {"build_silver"},
    "build_gold": {"quality_gate"},
}


@pytest.fixture(scope="module")
def bundle() -> dict:
    return yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def job(bundle) -> dict:
    return bundle["resources"]["jobs"]["ecommerce_pipeline"]


@pytest.fixture(scope="module")
def tareas(job) -> dict[str, dict]:
    return {t["task_key"]: t for t in job["tasks"]}


@pytest.fixture(scope="module")
def todas_las_tareas(bundle) -> list[tuple[str, dict]]:
    """Tareas de todos los jobs, no solo del pipeline.

    El bundle define además la demo de time travel, que también referencia un
    notebook y también se rompe si alguien lo renombra.
    """
    return [
        (f"{nombre}.{t['task_key']}", t)
        for nombre, definicion in bundle["resources"]["jobs"].items()
        for t in definicion["tasks"]
    ]


class TestDefinicionDelBundle:
    def test_el_fichero_es_yaml_valido(self, bundle) -> None:
        assert bundle["bundle"]["name"] == "ecommerce-lakehouse"

    def test_declara_los_entornos_de_desarrollo_y_produccion(self, bundle) -> None:
        assert set(bundle["targets"]) == {"dev", "prod"}
        assert bundle["targets"]["dev"]["mode"] == "development"
        assert bundle["targets"]["prod"]["mode"] == "production"

    def test_no_codifica_ningun_workspace(self, bundle) -> None:
        """Un host escrito aquí ataría el repositorio a una cuenta concreta y
        obligaría a editarlo para desplegarlo en otra."""
        texto = BUNDLE.read_text(encoding="utf-8")
        assert not re.search(r"https://[a-z0-9-]+\.cloud\.databricks\.com", texto)


class TestPlanificacion:
    """El pipeline corre solo, a diario.

    Un job programado consume cuota sin que nadie lo mire, así que por defecto
    queda en pausa y se activa de forma explícita. Que esté *definido* y en
    pausa es muy distinto de que no exista: la definición viaja en el
    repositorio y activarla es cambiar una variable.
    """

    def test_define_una_planificacion_diaria(self, bundle, job) -> None:
        assert "schedule" in job
        assert job["schedule"]["timezone_id"] == "${var.schedule_timezone}"

        # El cron vive en una variable, así que se comprueba su valor por
        # defecto: cinco espacios son los seis campos de Quartz (segundo,
        # minuto, hora, día del mes, mes, día de la semana).
        cron = bundle["variables"]["schedule_cron"]["default"]
        assert cron.count(" ") == 5, f"'{cron}' no es una expresión de Quartz"

    def test_arranca_en_pausa_por_defecto(self, bundle, job) -> None:
        """En Free Edition la cuota es limitada: activarse sin pedirlo sería
        gastar cómputo sin que nadie esté mirando el resultado."""
        assert job["schedule"]["pause_status"] == "${var.schedule_status}"
        assert bundle["variables"]["schedule_status"]["default"] == "PAUSED"

    def test_produccion_la_activa(self, bundle) -> None:
        assert bundle["targets"]["prod"]["variables"]["schedule_status"] == "UNPAUSED"


class TestGrafoDeTareas:
    def test_estan_las_cuatro_etapas(self, tareas) -> None:
        assert set(tareas) == set(DAG_ESPERADO)

    def test_las_dependencias_son_las_esperadas(self, tareas) -> None:
        for clave, esperadas in DAG_ESPERADO.items():
            reales = {d["task_key"] for d in tareas[clave].get("depends_on", [])}
            assert reales == esperadas, f"{clave} depende de {reales}, se esperaba {esperadas}"

    def test_gold_depende_de_la_puerta_y_no_de_silver(self, tareas) -> None:
        """Es la dependencia que da sentido a la puerta.

        Colgando Gold directamente de Silver, el pipeline publicaría cifras
        aunque la calidad hubiera fallado: la puerta quedaría como un aviso
        decorativo en vez de una parada.
        """
        assert {d["task_key"] for d in tareas["build_gold"]["depends_on"]} == {"quality_gate"}

    def test_toda_dependencia_apunta_a_una_tarea_existente(self, tareas) -> None:
        for clave, tarea in tareas.items():
            for dependencia in tarea.get("depends_on", []):
                assert dependencia["task_key"] in tareas, (
                    f"{clave} depende de '{dependencia['task_key']}', que no existe"
                )

    def test_el_grafo_no_tiene_ciclos(self, tareas) -> None:
        pendientes = {
            c: {d["task_key"] for d in t.get("depends_on", [])} for c, t in tareas.items()
        }
        resueltas: set[str] = set()
        while pendientes:
            listas = {c for c, deps in pendientes.items() if deps <= resueltas}
            assert listas, f"Ciclo entre las tareas: {sorted(pendientes)}"
            resueltas |= listas
            pendientes = {c: d for c, d in pendientes.items() if c not in listas}


class TestNotebooksReferenciados:
    def test_todos_los_notebooks_existen_en_el_repositorio(self, todas_las_tareas) -> None:
        """El fallo más probable al reorganizar el repositorio.

        Renombrar un notebook sin tocar el bundle produce un despliegue que
        parece correcto y un job que falla al ejecutarse.
        """
        for clave, tarea in todas_las_tareas:
            ruta = RAIZ / tarea["notebook_task"]["notebook_path"]
            assert ruta.is_file(), f"{clave} apunta a {ruta}, que no existe"

    def test_los_notebooks_llevan_la_cabecera_de_databricks(self, todas_las_tareas) -> None:
        """Sin `# Databricks notebook source`, el fichero se sube como fichero
        plano y la tarea falla con un 'no existe' desconcertante."""
        for clave, tarea in todas_las_tareas:
            ruta = RAIZ / tarea["notebook_task"]["notebook_path"]
            primera = ruta.read_text(encoding="utf-8").splitlines()[0]
            assert primera == "# Databricks notebook source", f"{clave}: {ruta.name}"

    def test_toda_tarea_declara_el_entorno_con_el_wheel(self, bundle) -> None:
        """Sin `environment_key`, la tarea corre sin el paquete instalado y
        falla al importar `ecommerce`."""
        for nombre, definicion in bundle["resources"]["jobs"].items():
            entornos = {e["environment_key"] for e in definicion.get("environments", [])}
            assert entornos, f"{nombre} no declara ningún entorno"
            for tarea in definicion["tasks"]:
                assert tarea.get("environment_key") in entornos, (
                    f"{nombre}.{tarea['task_key']} no usa un entorno declarado"
                )

    def test_la_demo_destructiva_no_esta_planificada(self, bundle) -> None:
        """Rompe una tabla Gold a propósito. Que se ejecutara sola sería
        exactamente el incidente que pretende enseñar a resolver."""
        demo = bundle["resources"]["jobs"]["time_travel_demo"]
        assert "schedule" not in demo


class TestDashboard:
    """El dashboard también es código y se verifica como tal."""

    @pytest.fixture
    def recurso(self, bundle) -> dict:
        return bundle["resources"]["dashboards"]["ecommerce_overview"]

    @pytest.fixture
    def definicion(self, recurso) -> dict:
        import json

        return json.loads((RAIZ / recurso["file_path"]).read_text(encoding="utf-8"))

    def test_el_fichero_de_definicion_existe(self, recurso) -> None:
        assert (RAIZ / recurso["file_path"]).is_file()

    def test_el_warehouse_se_resuelve_por_nombre(self, bundle, recurso) -> None:
        """Un identificador de warehouse es distinto en cada workspace.
        Codificarlo ataría el repositorio a esta cuenta."""
        assert recurso["warehouse_id"] == "${var.warehouse_id}"
        assert "lookup" in bundle["variables"]["warehouse_id"]

    def test_tiene_datasets_y_al_menos_una_pagina(self, definicion) -> None:
        assert definicion["datasets"]
        assert definicion["pages"]

    def test_el_dashboard_solo_lee_capas_de_consumo(self, definicion) -> None:
        """Un dashboard que consulta Silver o Bronze se salta la capa que da
        sentido al Medallion: acabaría reimplementando reglas de negocio en SQL
        de presentación, donde nadie las prueba."""
        for dataset in definicion["datasets"]:
            consulta = "".join(dataset["queryLines"]).lower()
            assert ".silver." not in consulta, dataset["name"]
            assert ".bronze." not in consulta, dataset["name"]
            assert ".gold." in consulta or ".ops." in consulta, dataset["name"]

    def test_cada_widget_referencia_un_dataset_declarado(self, definicion) -> None:
        declarados = {d["name"] for d in definicion["datasets"]}
        for pagina in definicion["pages"]:
            for elemento in pagina["layout"]:
                for consulta in elemento["widget"].get("queries", []):
                    nombre = consulta["query"]["datasetName"]
                    assert nombre in declarados, f"{elemento['widget']['name']} usa '{nombre}'"

    def test_cada_campo_del_widget_existe_en_su_dataset(self, definicion) -> None:
        """El error más probable al editar una consulta: renombrar una columna y
        dejar el widget apuntando a la anterior. El dashboard se despliega
        igual y el panel aparece vacío."""
        consultas = {d["name"]: "".join(d["queryLines"]) for d in definicion["datasets"]}

        for pagina in definicion["pages"]:
            for elemento in pagina["layout"]:
                for consulta in elemento["widget"].get("queries", []):
                    sql = consultas[consulta["query"]["datasetName"]]
                    for campo in consulta["query"]["fields"]:
                        assert campo["name"] in sql, (
                            f"{elemento['widget']['name']} usa el campo "
                            f"'{campo['name']}', que su consulta no produce"
                        )

    def test_el_json_no_se_ha_desincronizado_de_su_declaracion(self, definicion) -> None:
        """El JSON se genera desde `build_dashboard.py`.

        Editarlo a mano y olvidar regenerarlo dejaría la declaración mintiendo
        sobre lo que se despliega.
        """
        import sys

        sys.path.insert(0, str(RAIZ / "dashboards"))
        import build_dashboard

        assert build_dashboard.construir() == definicion

    def test_todo_widget_de_datos_lleva_titulo(self, definicion) -> None:
        """Un contador sin título es un número suelto que no indica nada.

        Fue el fallo de la primera versión: tres KPIs enormes sin una palabra
        que dijera qué medían.
        """
        for pagina in definicion["pages"]:
            for elemento in pagina["layout"]:
                spec = elemento["widget"].get("spec")
                if spec is None:  # cajas de texto
                    continue
                marco = spec.get("frame", {})
                assert marco.get("showTitle") and marco.get("title"), (
                    f"{elemento['widget']['name']} no muestra título"
                )

    def test_las_columnas_de_tabla_llevan_las_claves_que_databricks_exige(self, definicion) -> None:
        """Omitir una sola clave invalida el widget entero.

        Y no falla al desplegar: la validación ocurre al renderizar, así que el
        panel aparece con "Invalid widget definition is imported" y el
        despliegue parece correcto.
        """
        obligatorias = {
            "fieldName",
            "title",
            "type",
            "displayAs",
            "alignContent",
            "order",
            "visible",
            "booleanValues",
            "imageUrlTemplate",
            "imageTitleTemplate",
            "imageWidth",
            "imageHeight",
            "linkUrlTemplate",
            "linkTextTemplate",
            "linkTitleTemplate",
            "linkOpenInNewTab",
        }
        for pagina in definicion["pages"]:
            for elemento in pagina["layout"]:
                spec = elemento["widget"].get("spec", {})
                if spec.get("widgetType") != "table":
                    continue
                for columna in spec["encodings"]["columns"]:
                    faltan = obligatorias - set(columna)
                    assert not faltan, (
                        f"{elemento['widget']['name']}.{columna.get('fieldName')} "
                        f"no declara: {sorted(faltan)}"
                    )

    def test_los_widgets_no_se_solapan(self, definicion) -> None:
        """Dos widgets en la misma celda se pisan en el panel."""
        for pagina in definicion["pages"]:
            ocupadas: set[tuple[int, int]] = set()
            for elemento in pagina["layout"]:
                p = elemento["position"]
                celdas = {
                    (x, y)
                    for x in range(p["x"], p["x"] + p["width"])
                    for y in range(p["y"], p["y"] + p["height"])
                }
                solape = ocupadas & celdas
                assert not solape, f"{elemento['widget']['name']} se solapa en {sorted(solape)[:3]}"
                ocupadas |= celdas


class TestParametros:
    def test_todo_parametro_referenciado_esta_declarado(self, job, tareas) -> None:
        """Una referencia a un parámetro inexistente no falla al desplegar: se
        propaga como texto literal y el notebook recibe `{{job.parameters.x}}`
        como si fuera un valor."""
        declarados = {p["name"] for p in job.get("parameters", [])}

        for clave, tarea in tareas.items():
            for valor in tarea["notebook_task"].get("base_parameters", {}).values():
                for referencia in re.findall(r"\{\{job\.parameters\.([a-z_]+)\}\}", str(valor)):
                    assert referencia in declarados, (
                        f"{clave} usa el parámetro '{referencia}', que el job no declara"
                    )

    def test_todas_las_tareas_comparten_el_identificador_de_ejecucion(self, tareas) -> None:
        """Enlaza cada fila escrita con la corrida concreta que la produjo.

        Se excluye `build_gold`, que no escribe linaje: solo agrega lo que
        Silver ya dejó trazado.
        """
        for clave in ("ingest_bronze", "build_silver", "quality_gate"):
            parametros = tareas[clave]["notebook_task"]["base_parameters"]
            assert parametros.get("run_id") == "{{job.run_id}}", clave
