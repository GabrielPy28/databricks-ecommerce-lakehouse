"""Test de humo del entorno.

Verifica que el contenedor está bien construido antes de que valga la pena
depurar cualquier otra cosa. Si estos tests fallan, ningún otro fallo de la
suite es informativo.
"""

from __future__ import annotations

import shutil
import subprocess
import sys

import pytest


def test_python_es_312() -> None:
    """El runtime de Databricks usa 3.12; la paridad es el motivo del contenedor."""
    assert sys.version_info[:2] == (3, 12), f"Se esperaba Python 3.12, hay {sys.version}"


def test_java_disponible() -> None:
    """PySpark no arranca sin un runtime de Java en el PATH."""
    assert shutil.which("java") is not None, "Java no está en el PATH"


def test_databricks_cli_soporta_bundles() -> None:
    """Comprueba que está el CLI moderno, no el antiguo de Python.

    El CLI antiguo (`pip install databricks-cli`) también responde a
    `databricks --version`, pero carece del subcomando `bundle`. Sin él no hay
    Asset Bundles, que es como se despliegan los Workflows del proyecto.
    """
    assert shutil.which("databricks") is not None, "El CLI de Databricks no está instalado"

    result = subprocess.run(
        ["databricks", "bundle", "--help"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        "`databricks bundle` no está disponible. Probablemente se instaló el CLI "
        f"antiguo de Python en lugar del binario en Go. Salida: {result.stderr}"
    )


@pytest.mark.spark
def test_spark_arranca(spark) -> None:
    """La sesión de Spark se crea y ejecuta una consulta trivial."""
    # Se fija 4.1.x a propósito: es la versión de Spark del runtime más alto
    # disponible en el workspace (DBR 18.2). Si alguien actualiza pyspark por
    # delante de Databricks, este test lo detecta antes que un despliegue.
    assert spark.version.startswith("4.1."), (
        f"Se esperaba Spark 4.1.x para mantener paridad con DBR 18.2, hay {spark.version}"
    )
    assert spark.range(10).count() == 10


@pytest.mark.spark
def test_delta_escribe_lee_y_versiona(spark, tmp_path) -> None:
    """Delta Lake funciona de extremo a extremo: ACID, actualización y time travel.

    Este es el test que de verdad importa: confirma que los JAR de Delta están
    presentes y emparejados con la versión de PySpark. Un desajuste entre
    `pyspark` y `delta-spark` no se manifiesta al importar, sino aquí.
    """
    ruta = str(tmp_path / "tabla_delta")

    # Versión 0
    spark.range(5).write.format("delta").save(ruta)
    assert spark.read.format("delta").load(ruta).count() == 5

    # Versión 1
    spark.range(5, 10).write.format("delta").mode("append").save(ruta)
    assert spark.read.format("delta").load(ruta).count() == 10

    # Time travel: la versión 0 sigue siendo recuperable.
    anterior = spark.read.format("delta").option("versionAsOf", 0).load(ruta)
    assert anterior.count() == 5, "Time travel no devolvió el estado original"
