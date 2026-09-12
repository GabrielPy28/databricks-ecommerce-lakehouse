"""Transformaciones de Silver: normalización, tipado, cuarentena y MERGE.

Estos tests son el corazón del proyecto. Delta Lake funciona en el contenedor,
así que el `MERGE` —la pieza de la que depende la idempotencia y el tratamiento
de datos tardíos— se prueba de verdad en local, no solo en el workspace.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from delta.tables import DeltaTable

from ecommerce.schemas import spark as schema_spark
from ecommerce.transformations import cleaning, merge, silver

pytestmark = pytest.mark.spark


# Órdenes tal como llegan de Bronze: todo texto.
FILAS_ATERRIZAJE = [
    ("ORD-1", "CUS-1", "2026-01-15 10:30:00", " DELIVERED ", "12.50", "2026-01-15 10:30:00"),
    ("ORD-2", "CUS-2", "2026-01-16 11:00:00", "paid", "30.00", "2026-01-16 11:00:00"),
    ("ORD-3", "CUS-3", "not-a-date", "shipped", "45.00", "2026-01-17 09:00:00"),
]
ESQUEMA_ATERRIZAJE = (
    "order_id string, customer_id string, order_date string, "
    "status string, total_amount string, updated_at string"
)


@pytest.fixture
def aterrizaje(spark):
    return spark.createDataFrame(FILAS_ATERRIZAJE, ESQUEMA_ATERRIZAJE)


class TestConversionDeEsquema:
    def test_traduce_el_contrato_de_arrow_a_tipos_de_spark(self) -> None:
        """El contrato se declara una sola vez, en pyarrow, y se traduce.

        Declararlo dos veces —una para el generador y otra para Spark— sería
        garantía de que ambas versiones divergen con el tiempo.
        """
        esquema = schema_spark.to_spark_schema("orders")
        tipos = {campo.name: campo.dataType.simpleString() for campo in esquema.fields}
        assert tipos["order_id"] == "string"
        assert tipos["order_date"] == "timestamp"
        assert tipos["total_amount"] == "decimal(12,2)"

    def test_la_cantidad_de_linea_es_entera(self) -> None:
        esquema = schema_spark.to_spark_schema("order_items")
        tipos = {campo.name: campo.dataType.simpleString() for campo in esquema.fields}
        assert tipos["quantity"] == "int"


class TestNormalizacion:
    def test_unifica_mayusculas_y_espacios_del_estado(self, aterrizaje) -> None:
        """El generador inyecta ` DELIVERED ` a propósito. Sin normalizar, la
        validación de dominio lo rechazaría siendo un valor perfectamente
        legítimo mal escrito."""
        resultado = cleaning.normalize_status(aterrizaje, "status")
        assert [f.status for f in resultado.collect()] == ["delivered", "paid", "shipped"]

    def test_recorta_los_espacios_de_las_columnas_de_texto(self, spark) -> None:
        df = spark.createDataFrame([("  ORD-1  ",)], "order_id string")
        resultado = cleaning.trim_strings(df, ["order_id"])
        assert resultado.first().order_id == "ORD-1"


class TestTipado:
    def test_convierte_texto_a_los_tipos_del_contrato(self, aterrizaje) -> None:
        resultado = cleaning.cast_to_contract(aterrizaje, "orders")
        fila = resultado.filter("order_id = 'ORD-2'").first()
        assert fila.total_amount == Decimal("30.00")
        assert fila.order_date.year == 2026

    def test_un_valor_invalido_se_vuelve_nulo_en_lugar_de_romper(self, aterrizaje) -> None:
        """Se usa `try_cast` y no `cast`.

        Spark 4 activa el modo ANSI por defecto, con el que un `cast` inválido
        lanza excepción y aborta el job entero. Un solo registro malo entre
        millones tumbaría el pipeline. `try_cast` lo convierte en nulo, que la
        cuarentena recoge después.
        """
        resultado = cleaning.cast_to_contract(aterrizaje, "orders")
        assert resultado.filter("order_id = 'ORD-3'").first().order_date is None

    def test_registra_que_columnas_fallaron_al_convertir(self, aterrizaje) -> None:
        """Un nulo tras convertir puede venir de un origen nulo o de un valor
        corrupto. Distinguirlos es lo que permite explicar la cuarentena."""
        resultado = cleaning.with_cast_errors(aterrizaje, "orders")
        errores = {f.order_id: f._cast_errors for f in resultado.collect()}
        assert errores["ORD-3"] == ["order_date"]
        assert errores["ORD-1"] == []


class TestCuarentena:
    def test_separa_las_filas_corruptas_de_las_validas(self, aterrizaje) -> None:
        validas, cuarentena = silver.split_quarantine(
            cleaning.with_cast_errors(aterrizaje, "orders")
        )
        assert validas.count() == 2
        assert cuarentena.count() == 1
        assert cuarentena.first().order_id == "ORD-3"

    def test_no_pierde_ninguna_fila(self, aterrizaje) -> None:
        """Cuarentena, no descarte: toda fila de entrada acaba en algún sitio."""
        validas, cuarentena = silver.split_quarantine(
            cleaning.with_cast_errors(aterrizaje, "orders")
        )
        assert validas.count() + cuarentena.count() == aterrizaje.count()

    def test_la_cuarentena_conserva_el_motivo(self, aterrizaje) -> None:
        _, cuarentena = silver.split_quarantine(cleaning.with_cast_errors(aterrizaje, "orders"))
        assert silver.ERRORS in cuarentena.columns
        assert cuarentena.first()[silver.ERRORS] == ["order_date"]


class TestCuarentenaUnificada:
    """Fallos de conversión y de reglas comparten destino.

    Mantener dos mecanismos en paralelo obligaría a mirar en dos sitios para
    saber por qué se rechazó una fila, y a reconciliar dos recuentos que
    deberían ser uno.
    """

    @pytest.fixture
    def con_ambos_errores(self, spark, aterrizaje):
        from ecommerce.quality import engine as motor
        from ecommerce.quality.rules import Rule

        # La normalización va ANTES de validar, igual que en el pipeline real:
        # ` DELIVERED ` es un valor legítimo mal escrito, y comprobar el
        # dominio sin unificarlo primero lo rechazaría por su forma en vez de
        # por su contenido.
        normalizado = cleaning.normalize_status(aterrizaje, "status")
        tipado = cleaning.with_cast_errors(normalizado, "orders")
        regla = Rule("silver.orders", "status", "in_set", "quarantine", {"values": ["delivered"]})
        return motor.evaluate(tipado, [regla])[0]

    def test_reune_los_dos_tipos_de_error_en_una_columna(self, con_ambos_errores) -> None:
        combinado = silver.combine_errors(con_ambos_errores)
        assert silver.ERRORS in combinado.columns
        assert cleaning.CAST_ERRORS not in combinado.columns

    def test_una_fila_puede_acumular_errores_de_ambos_origenes(self, con_ambos_errores) -> None:
        """ORD-3 trae una fecha imposible de convertir Y un estado fuera del
        dominio. Perder uno de los dos motivos dejaría el diagnóstico a medias."""
        combinado = silver.combine_errors(con_ambos_errores)
        errores = combinado.filter("order_id = 'ORD-3'").first()[silver.ERRORS]
        assert "order_date" in errores
        assert "status:in_set" in errores

    def test_aparta_toda_fila_con_cualquier_tipo_de_error(self, con_ambos_errores) -> None:
        validas, cuarentena = silver.split_quarantine(con_ambos_errores)
        # ORD-1 (delivered) es la única que supera conversión y reglas.
        assert validas.count() == 1
        assert cuarentena.count() == 2


class TestDeduplicacion:
    def test_de_varias_versiones_conserva_la_mas_reciente(self, spark) -> None:
        df = spark.createDataFrame(
            [
                ("ORD-1", "pending", "2026-01-01 00:00:00"),
                ("ORD-1", "delivered", "2026-01-05 00:00:00"),
                ("ORD-1", "shipped", "2026-01-03 00:00:00"),
            ],
            "order_id string, status string, updated_at string",
        )
        resultado = merge.deduplicate_latest(df, key="order_id", order_by="updated_at")
        assert resultado.count() == 1
        assert resultado.first().status == "delivered"

    def test_con_clave_compuesta_conserva_una_fila_por_combinacion(self, spark) -> None:
        """Lo necesitan las dimensiones SCD2.

        Deduplicar por la clave de negocio a secas se quedaría solo con la
        versión más reciente y destruiría el historial. Con la clave y la marca
        de tiempo, se conserva una fila por versión.
        """
        df = spark.createDataFrame(
            [
                ("CUS-1", "MX", "2026-01-01 00:00:00"),
                ("CUS-1", "MX", "2026-01-01 00:00:00"),
                ("CUS-1", "ES", "2026-03-01 00:00:00"),
            ],
            "customer_id string, country string, updated_at string",
        )
        resultado = merge.deduplicate_latest(
            df, key=["customer_id", "updated_at"], order_by="updated_at"
        )
        assert resultado.count() == 2

    def test_un_duplicado_exacto_colapsa_a_una_fila(self, spark) -> None:
        """Es el caso que inyecta el generador: el origen reemite el registro."""
        df = spark.createDataFrame(
            [("ORD-1", "paid", "2026-01-01 00:00:00")] * 2,
            "order_id string, status string, updated_at string",
        )
        assert merge.deduplicate_latest(df, key="order_id", order_by="updated_at").count() == 1


class TestMergeIdempotente:
    """La propiedad que el proyecto promete: ejecutar dos veces no cambia nada.

    Es la pregunta que se hace en entrevista, y aquí queda demostrada en lugar
    de afirmada.
    """

    @pytest.fixture
    def destino(self, spark, tmp_path):
        ruta = str(tmp_path / "orders_delta")
        spark.createDataFrame(
            [("ORD-1", "pending", "2026-01-01 00:00:00")],
            "order_id string, status string, updated_at string",
        ).write.format("delta").save(ruta)
        return DeltaTable.forPath(spark, ruta), ruta

    def _leer(self, spark, ruta):
        return spark.read.format("delta").load(ruta)

    def test_inserta_las_ordenes_que_no_existian(self, spark, destino) -> None:
        tabla, ruta = destino
        nueva = spark.createDataFrame(
            [("ORD-2", "paid", "2026-01-02 00:00:00")],
            "order_id string, status string, updated_at string",
        )
        merge.merge_latest(tabla, nueva, key="order_id", order_by="updated_at")
        assert self._leer(spark, ruta).count() == 2

    def test_ejecutar_el_mismo_lote_dos_veces_no_duplica(self, spark, destino) -> None:
        tabla, ruta = destino
        lote = spark.createDataFrame(
            [("ORD-2", "paid", "2026-01-02 00:00:00")],
            "order_id string, status string, updated_at string",
        )
        merge.merge_latest(tabla, lote, key="order_id", order_by="updated_at")
        merge.merge_latest(tabla, lote, key="order_id", order_by="updated_at")

        resultado = self._leer(spark, ruta)
        assert resultado.count() == 2
        ids = [f.order_id for f in resultado.collect()]
        assert len(ids) == len(set(ids))

    def test_una_version_mas_reciente_actualiza_la_fila(self, spark, destino) -> None:
        """Una orden muta: pending -> paid -> shipped -> delivered."""
        tabla, ruta = destino
        actualizada = spark.createDataFrame(
            [("ORD-1", "delivered", "2026-01-09 00:00:00")],
            "order_id string, status string, updated_at string",
        )
        merge.merge_latest(tabla, actualizada, key="order_id", order_by="updated_at")

        resultado = self._leer(spark, ruta)
        assert resultado.count() == 1
        assert resultado.first().status == "delivered"

    def test_un_dato_tardio_no_pisa_a_uno_mas_reciente(self, spark, destino) -> None:
        """El caso que rompe los pipelines ingenuos.

        Un registro que pertenece al pasado pero llega ahora no debe revertir
        el estado actual. Sin la condición sobre `updated_at`, este MERGE
        devolvería la orden a `pending` y el dato quedaría corrupto de forma
        silenciosa.
        """
        tabla, ruta = destino

        # Primero avanza al estado actual.
        merge.merge_latest(
            tabla,
            spark.createDataFrame(
                [("ORD-1", "delivered", "2026-01-09 00:00:00")],
                "order_id string, status string, updated_at string",
            ),
            key="order_id",
            order_by="updated_at",
        )
        # Ahora llega, tarde, una versión anterior.
        merge.merge_latest(
            tabla,
            spark.createDataFrame(
                [("ORD-1", "pending", "2026-01-01 00:00:00")],
                "order_id string, status string, updated_at string",
            ),
            key="order_id",
            order_by="updated_at",
        )

        assert self._leer(spark, ruta).first().status == "delivered"

    def test_deduplica_el_origen_antes_de_mezclar(self, spark, destino) -> None:
        """Delta falla si el origen trae varias filas para la misma clave.

        El generador inyecta duplicados a propósito, así que el MERGE tiene que
        deduplicar antes o el pipeline se cae en cuanto llega un lote real.
        """
        tabla, ruta = destino
        con_duplicados = spark.createDataFrame(
            [
                ("ORD-2", "pending", "2026-01-02 00:00:00"),
                ("ORD-2", "paid", "2026-01-04 00:00:00"),
            ],
            "order_id string, status string, updated_at string",
        )
        merge.merge_latest(tabla, con_duplicados, key="order_id", order_by="updated_at")

        resultado = self._leer(spark, ruta).filter("order_id = 'ORD-2'")
        assert resultado.count() == 1
        assert resultado.first().status == "paid"
