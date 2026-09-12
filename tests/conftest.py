"""Fixtures compartidas por la suite de tests."""

from __future__ import annotations

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    """Sesión de Spark local con Delta Lake activado.

    De alcance `session`: arrancar Spark cuesta varios segundos y hacerlo una
    vez por test volvería la suite inutilizablemente lenta.

    El warehouse apunta a un directorio temporal para que los tests no dejen
    `spark-warehouse/` ni `metastore_db/` en el árbol de trabajo.
    """
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    derby = tmp_path_factory.mktemp("derby")

    builder = (
        SparkSession.builder.appName("ecommerce-tests")
        # local[2]: suficiente paralelismo para que aflore cualquier problema
        # de partición, sin el coste de arrancar más ejecutores.
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby}")
        # Por defecto son 200 particiones. Sobre datos de test eso genera
        # cientos de ficheros diminutos y multiplica el tiempo de ejecución.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        # Evita que Spark reserve un puerto para el servidor de la interfaz.
        .config("spark.sql.session.timeZone", "UTC")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    yield session

    session.stop()
