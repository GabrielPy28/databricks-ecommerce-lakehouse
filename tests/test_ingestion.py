"""Ingesta a Bronze.

`cloudFiles` (Auto Loader) es exclusivo de Databricks y no se puede ejecutar en
el contenedor. Por eso la ingesta se parte en dos: lo que es lógica pura —las
opciones del lector y el añadido de linaje— se prueba aquí, y la lectura en
streaming queda como envoltura fina que se valida ejecutándola en el workspace.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ecommerce.ingestion import autoloader, bronze
from ecommerce.schemas import metadata

pytestmark = pytest.mark.spark


class TestOpcionesDeAutoLoader:
    def test_usa_formato_parquet(self) -> None:
        opciones = autoloader.options(entity="orders", schema_location="/tmp/esquemas")
        assert opciones["cloudFiles.format"] == "parquet"

    def test_cada_entidad_tiene_su_propia_ubicacion_de_esquema(self) -> None:
        """El esquema y el checkpoint son por entidad.

        Compartirlos haría que Auto Loader mezclara el estado de `orders` con el
        de `customers` y diese por ingeridos ficheros que no lo están.
        """
        a = autoloader.options(entity="orders", schema_location="/tmp/esquemas")
        b = autoloader.options(entity="customers", schema_location="/tmp/esquemas")
        assert a["cloudFiles.schemaLocation"] != b["cloudFiles.schemaLocation"]

    def test_evoluciona_el_esquema_anadiendo_columnas(self) -> None:
        """Bronze preserva lo que llega: una columna nueva se añade, no rompe."""
        opciones = autoloader.options(entity="orders", schema_location="/tmp/esquemas")
        assert opciones["cloudFiles.schemaEvolutionMode"] == "addNewColumns"

    def test_rescata_lo_que_no_encaja_en_el_esquema(self) -> None:
        """Sin columna de rescate, un valor inesperado se perdería en silencio,
        que es justo lo contrario de lo que Bronze promete."""
        opciones = autoloader.options(entity="orders", schema_location="/tmp/esquemas")
        assert opciones["cloudFiles.rescuedDataColumn"] == "_rescued_data"

    def test_infiere_las_columnas_como_texto(self) -> None:
        """Los ficheros de aterrizaje ya son todo texto y el tipado es trabajo
        de Silver. Inferir tipos aquí adelantaría decisiones a la capa
        equivocada y haría fallar la ingesta ante un valor inválido."""
        opciones = autoloader.options(entity="orders", schema_location="/tmp/esquemas")
        assert opciones["cloudFiles.inferColumnTypes"] == "false"


class TestLinaje:
    @pytest.fixture
    def df_desde_fichero(self, spark, tmp_path: Path):
        """Un DataFrame leído de un fichero real.

        Hace falta que provenga de un fichero: `_source_file` se toma de los
        metadatos del origen, que no existen en un DataFrame creado en memoria.
        """
        ruta = tmp_path / "datos"
        spark.createDataFrame(
            [("ORD-1", "10.00"), ("ORD-2", "20.00")], "order_id string, total string"
        ).write.parquet(str(ruta))
        return spark.read.parquet(str(ruta))

    def test_anade_las_cuatro_columnas_de_linaje(self, df_desde_fichero) -> None:
        resultado = bronze.add_lineage(df_desde_fichero, run_id="run-1")
        for columna in metadata.LINEAGE_COLUMNS:
            assert columna in resultado.columns

    def test_conserva_las_columnas_de_negocio_y_las_filas(self, df_desde_fichero) -> None:
        """Bronze preserva. Perder una fila o una columna aquí sería un fallo
        de la promesa central de la capa."""
        resultado = bronze.add_lineage(df_desde_fichero, run_id="run-1")
        assert set(df_desde_fichero.columns) <= set(resultado.columns)
        assert resultado.count() == df_desde_fichero.count()

    def test_registra_la_ejecucion_recibida(self, df_desde_fichero) -> None:
        resultado = bronze.add_lineage(df_desde_fichero, run_id="run-42")
        assert resultado.first()[metadata.PIPELINE_RUN_ID] == "run-42"

    def test_registra_el_fichero_de_origen(self, df_desde_fichero) -> None:
        """Permite rastrear cualquier fila hasta el fichero del que salió."""
        resultado = bronze.add_lineage(df_desde_fichero, run_id="run-1")
        assert resultado.first()[metadata.SOURCE_FILE].endswith(".parquet")

    def test_la_marca_de_ingesta_no_es_nula(self, df_desde_fichero) -> None:
        resultado = bronze.add_lineage(df_desde_fichero, run_id="run-1")
        assert resultado.first()[metadata.INGESTION_TIMESTAMP] is not None


class TestEtiquetadoDelLote:
    """El lote se deduce de la ruta del fichero, no de un parámetro.

    Auto Loader ingiere **todos** los ficheros nuevos del directorio que vigila,
    sin saber nada del parámetro con el que se invocó el job. Etiquetar con el
    parámetro produce etiquetas que mienten: ocurrió de verdad, con los dos
    lotes marcados como `batch_000`.

    Importa porque `build_gold` deriva de `_batch_id` los días que recalcula.
    Una etiqueta equivocada no rompe nada: deja Gold desactualizado en silencio,
    que es la peor clase de fallo de este proyecto.
    """

    @pytest.mark.parametrize(
        ("ruta", "esperado"),
        [
            ("dbfs:/Volumes/c/landing/raw/orders/batch_007/part-00000.parquet", "batch_007"),
            ("/Volumes/c/landing/raw/web_events/batch_031/part-00012.parquet", "batch_031"),
            ("s3://bucket/raw/customers/batch_000/x.parquet", "batch_000"),
        ],
    )
    def test_extrae_el_lote_de_la_ruta(self, ruta: str, esperado: str) -> None:
        assert bronze.batch_from_path(ruta) == esperado

    @pytest.mark.parametrize(
        "ruta",
        ["/datos/sueltos/fichero.parquet", "/Volumes/c/landing/raw/orders/x.parquet", ""],
    )
    def test_una_ruta_sin_lote_no_inventa_uno(self, ruta: str) -> None:
        """Devolver algo plausible sería peor: `build_gold` recalcularía una
        ventana equivocada creyendo que la etiqueta es buena."""
        assert bronze.batch_from_path(ruta) is None

    def test_la_columna_de_lote_sale_de_la_ruta_real(self, spark, tmp_path: Path) -> None:
        destino = tmp_path / "orders" / "batch_042"
        destino.mkdir(parents=True)
        spark.createDataFrame([("ORD-1",)], "order_id string").write.parquet(str(destino / "datos"))

        leido = spark.read.parquet(str(destino / "datos"))
        resultado = bronze.add_lineage(leido, run_id="run-1")
        assert resultado.first()[metadata.BATCH_ID] == "batch_042"
