"""SCD Tipo 2: historial de las dimensiones.

Por qué hace falta, si `order_items.unit_price` ya preserva el precio de venta:
porque los ingresos históricos son correctos sin SCD2, pero el análisis por
segmento no. Un cliente que se muda de México a España reescribiría
retroactivamente los ingresos históricos de ambos países si la dimensión solo
guardara su estado actual.
"""

from __future__ import annotations

import itertools
from datetime import datetime, timedelta

import pytest
from delta.tables import DeltaTable

from ecommerce.transformations import scd2

pytestmark = pytest.mark.spark

ESQUEMA_ORIGEN = "customer_id string, country string, city string, updated_at timestamp"

T0 = datetime(2026, 1, 1)
T1 = datetime(2026, 3, 1)
T2 = datetime(2026, 6, 1)


@pytest.fixture
def destino(spark, tmp_path):
    """Tabla SCD2 con un cliente en su versión inicial."""
    ruta = str(tmp_path / "customers_scd2")
    inicial = spark.createDataFrame([("CUS-1", "MX", "CDMX", T0)], ESQUEMA_ORIGEN)
    scd2.initialize(inicial, ruta, key="customer_id", effective_from="updated_at")
    return DeltaTable.forPath(spark, ruta), ruta


def leer(spark, ruta):
    return spark.read.format("delta").load(ruta)


def aplicar(spark, tabla, ruta, filas):
    scd2.apply(
        tabla,
        spark.createDataFrame(filas, ESQUEMA_ORIGEN),
        key="customer_id",
        attributes=["country", "city"],
        effective_from="updated_at",
    )
    return leer(spark, ruta)


class TestCargaInicial:
    def test_la_version_inicial_queda_vigente_y_abierta(self, spark, destino) -> None:
        _, ruta = destino
        fila = leer(spark, ruta).first()
        assert fila.is_current is True
        assert fila.valid_to is None
        assert fila.valid_from == T0

    def test_anade_las_columnas_de_historial(self, spark, destino) -> None:
        _, ruta = destino
        for columna in (scd2.VALID_FROM, scd2.VALID_TO, scd2.IS_CURRENT):
            assert columna in leer(spark, ruta).columns


class TestCambioDeAtributo:
    def test_cierra_la_version_anterior_y_abre_una_nueva(self, spark, destino) -> None:
        tabla, ruta = destino
        resultado = aplicar(spark, tabla, ruta, [("CUS-1", "ES", "Madrid", T1)])

        assert resultado.count() == 2
        vigente = resultado.filter("is_current").first()
        cerrada = resultado.filter("NOT is_current").first()

        assert vigente.country == "ES"
        assert vigente.valid_from == T1
        assert vigente.valid_to is None

        assert cerrada.country == "MX"
        assert cerrada.valid_to == T1

    def test_el_historial_no_deja_huecos_ni_solapes(self, spark, destino) -> None:
        """El `valid_to` de una versión es exactamente el `valid_from` de la
        siguiente. Un hueco perdería órdenes al unir por fecha; un solape las
        contaría dos veces."""
        tabla, ruta = destino
        aplicar(spark, tabla, ruta, [("CUS-1", "ES", "Madrid", T1)])
        resultado = aplicar(spark, tabla, ruta, [("CUS-1", "US", "Austin", T2)])

        versiones = sorted(resultado.collect(), key=lambda f: f.valid_from)
        assert [v.country for v in versiones] == ["MX", "ES", "US"]
        for anterior, siguiente in itertools.pairwise(versiones):
            assert anterior.valid_to == siguiente.valid_from
        assert versiones[-1].valid_to is None

    def test_solo_una_version_queda_vigente(self, spark, destino) -> None:
        tabla, ruta = destino
        aplicar(spark, tabla, ruta, [("CUS-1", "ES", "Madrid", T1)])
        resultado = aplicar(spark, tabla, ruta, [("CUS-1", "US", "Austin", T2)])
        assert resultado.filter("is_current").count() == 1


class TestSinCambios:
    def test_una_fila_identica_no_genera_version_nueva(self, spark, destino) -> None:
        """Los lotes reenvían la dimensión completa en cada carga. Si cada
        reenvío abriera una versión, el historial crecería sin que nada hubiera
        cambiado y quedaría inservible."""
        tabla, ruta = destino
        resultado = aplicar(spark, tabla, ruta, [("CUS-1", "MX", "CDMX", T1)])
        assert resultado.count() == 1
        assert resultado.first().valid_from == T0

    def test_aplicar_el_mismo_lote_dos_veces_no_cambia_nada(self, spark, destino) -> None:
        tabla, ruta = destino
        aplicar(spark, tabla, ruta, [("CUS-1", "ES", "Madrid", T1)])
        antes = leer(spark, ruta).collect()

        resultado = aplicar(spark, tabla, ruta, [("CUS-1", "ES", "Madrid", T1)])
        assert resultado.count() == len(antes) == 2


class TestClavesNuevas:
    def test_una_clave_que_no_existia_se_inserta_vigente(self, spark, destino) -> None:
        tabla, ruta = destino
        resultado = aplicar(spark, tabla, ruta, [("CUS-2", "BR", "Sao Paulo", T1)])

        nueva = resultado.filter("customer_id = 'CUS-2'").first()
        assert nueva.is_current is True
        assert nueva.valid_from == T1
        # La existente no se toca.
        assert resultado.filter("customer_id = 'CUS-1' AND is_current").count() == 1


class TestValoresNulos:
    def test_un_atributo_que_pasa_a_nulo_cuenta_como_cambio(self, spark, destino) -> None:
        """Con una comparación normal, `MX != NULL` es NULL y no true, así que
        el cambio pasaría desapercibido. Hace falta igualdad segura ante nulos."""
        tabla, ruta = destino
        resultado = aplicar(spark, tabla, ruta, [("CUS-1", None, "CDMX", T1)])
        assert resultado.count() == 2
        assert resultado.filter("is_current").first().country is None

    def test_un_atributo_que_deja_de_ser_nulo_cuenta_como_cambio(self, spark, tmp_path) -> None:
        ruta = str(tmp_path / "scd2_nulos")
        inicial = spark.createDataFrame([("CUS-9", None, "Lima", T0)], ESQUEMA_ORIGEN)
        scd2.initialize(inicial, ruta, key="customer_id", effective_from="updated_at")
        tabla = DeltaTable.forPath(spark, ruta)

        resultado = aplicar(spark, tabla, ruta, [("CUS-9", "PE", "Lima", T1)])
        assert resultado.count() == 2


class TestReproduccionDelHistorial:
    """Reconstruir todo el historial desde un origen con varias versiones.

    Silver recorre todo Bronze en cada ejecución, y Bronze acumula una fila por
    cada vez que la dimensión cambió. Aplicar ese origen de golpe no funciona:
    el MERGE exige una sola fila por clave, y quedarse con la más reciente
    —que es lo que hace la deduplicación normal— **destruye el historial**,
    dejando la dimensión con una sola versión por clave.

    La solución es reproducir las versiones en orden cronológico.
    """

    @pytest.fixture
    def origen_con_tres_versiones(self, spark):
        return spark.createDataFrame(
            [
                ("CUS-1", "MX", "CDMX", T0),
                ("CUS-1", "ES", "Madrid", T1),
                ("CUS-1", "US", "Austin", T2),
                ("CUS-2", "BR", "Sao Paulo", T0),
            ],
            ESQUEMA_ORIGEN,
        )

    def test_reconstruye_todas_las_versiones(self, spark, tmp_path, origen_con_tres_versiones):
        ruta = str(tmp_path / "scd2_replay")
        vacia = spark.createDataFrame([], ESQUEMA_ORIGEN)
        scd2.initialize(vacia, ruta, key="customer_id", effective_from="updated_at")

        scd2.apply_history(
            DeltaTable.forPath(spark, ruta),
            origen_con_tres_versiones,
            key="customer_id",
            attributes=["country", "city"],
            effective_from="updated_at",
        )

        resultado = leer(spark, ruta)
        assert resultado.filter("customer_id = 'CUS-1'").count() == 3
        assert resultado.filter("customer_id = 'CUS-1' AND is_current").count() == 1
        assert resultado.filter("customer_id = 'CUS-1' AND is_current").first().country == "US"

    def test_reproducirlo_dos_veces_no_duplica_versiones(
        self, spark, tmp_path, origen_con_tres_versiones
    ):
        """Es lo que garantiza que reejecutar Silver sea seguro."""
        ruta = str(tmp_path / "scd2_replay_idem")
        vacia = spark.createDataFrame([], ESQUEMA_ORIGEN)
        scd2.initialize(vacia, ruta, key="customer_id", effective_from="updated_at")
        tabla = DeltaTable.forPath(spark, ruta)

        for _ in range(2):
            scd2.apply_history(
                tabla,
                origen_con_tres_versiones,
                key="customer_id",
                attributes=["country", "city"],
                effective_from="updated_at",
            )

        assert leer(spark, ruta).count() == 4


class TestCosteDeLaReproduccion:
    """El replay debe costar tantas pasadas como versiones, no como fechas.

    Nace de un fallo real: al marcar cada cliente con su fecha de alta, la
    dimensión pasó a tener una marca temporal distinta por fila. Agrupando el
    replay por marca temporal, la carga inicial de 1.000 clientes lanzaba unos
    2.000 MERGE y Silver pasó de 3 minutos a más de 50.

    Lo que obliga a separar pasadas es que una misma **clave** traiga varias
    versiones, no que haya muchas fechas distintas: filas de claves distintas
    caben en el mismo MERGE aunque sus fechas difieran.
    """

    def _historial(self, spark, ruta) -> int:
        return DeltaTable.forPath(spark, ruta).history().count()

    def test_muchas_fechas_distintas_no_multiplican_las_pasadas(self, spark, tmp_path) -> None:
        ruta = str(tmp_path / "scd2_coste")
        scd2.initialize(
            spark.createDataFrame([], ESQUEMA_ORIGEN),
            ruta,
            key="customer_id",
            effective_from="updated_at",
        )
        tabla = DeltaTable.forPath(spark, ruta)
        commits_iniciales = self._historial(spark, ruta)

        # 40 clientes, una versión cada uno, todas con fecha distinta.
        filas = [
            (f"CUS-{i:04d}", "MX", "CDMX", datetime(2026, 1, 1) + timedelta(days=i))
            for i in range(40)
        ]
        scd2.apply_history(
            tabla,
            spark.createDataFrame(filas, ESQUEMA_ORIGEN),
            key="customer_id",
            attributes=["country", "city"],
            effective_from="updated_at",
        )

        assert leer(spark, ruta).count() == 40

        # `apply` hace dos MERGE, así que una sola pasada deja como mucho dos
        # commits. Agrupando por fecha serían ochenta.
        commits = self._historial(spark, ruta) - commits_iniciales
        assert commits <= 2, f"{commits} commits: el replay agrupó por fecha, no por versión"

    def test_varias_versiones_de_una_clave_si_requieren_pasadas(self, spark, tmp_path) -> None:
        """La contrapartida: lo que obliga a separar pasadas es la clave
        repetida, y eso sigue funcionando."""
        ruta = str(tmp_path / "scd2_coste_versiones")
        scd2.initialize(
            spark.createDataFrame([], ESQUEMA_ORIGEN),
            ruta,
            key="customer_id",
            effective_from="updated_at",
        )
        tabla = DeltaTable.forPath(spark, ruta)

        filas = [
            ("CUS-1", "MX", "CDMX", T0),
            ("CUS-1", "ES", "Madrid", T1),
            ("CUS-1", "US", "Austin", T2),
        ]
        scd2.apply_history(
            tabla,
            spark.createDataFrame(filas, ESQUEMA_ORIGEN),
            key="customer_id",
            attributes=["country", "city"],
            effective_from="updated_at",
        )
        assert leer(spark, ruta).count() == 3


class TestUnionPuntualEnElTiempo:
    """Unir un hecho con la versión de la dimensión vigente cuando ocurrió.

    Es para lo que sirve el historial. Uniendo contra la versión actual, una
    compra de enero en México aparecería atribuida a España solo porque el
    cliente se mudó en marzo, y los ingresos históricos por país cambiarían
    cada vez que alguien se muda.
    """

    @pytest.fixture
    def dimension(self, spark, tmp_path):
        ruta = str(tmp_path / "scd2_join")
        scd2.initialize(
            spark.createDataFrame([("CUS-1", "MX", "CDMX", T0)], ESQUEMA_ORIGEN),
            ruta,
            key="customer_id",
            effective_from="updated_at",
        )
        tabla = DeltaTable.forPath(spark, ruta)
        aplicar(spark, tabla, ruta, [("CUS-1", "ES", "Madrid", T1)])
        return leer(spark, ruta)

    @pytest.fixture
    def hechos(self, spark):
        return spark.createDataFrame(
            [
                ("ORD-1", "CUS-1", datetime(2026, 2, 1)),
                ("ORD-2", "CUS-1", datetime(2026, 4, 1)),
            ],
            "order_id string, customer_id string, order_date timestamp",
        )

    def test_cada_hecho_toma_la_version_de_su_fecha(self, dimension, hechos) -> None:
        resultado = scd2.join_as_of(
            hechos, dimension, key="customer_id", timestamp_column="order_date"
        )
        paises = {f.order_id: f.country for f in resultado.collect()}
        assert paises == {"ORD-1": "MX", "ORD-2": "ES"}

    def test_no_duplica_hechos(self, dimension, hechos) -> None:
        """Un solape en el historial multiplicaría las filas de hechos y los
        ingresos se contarían varias veces."""
        resultado = scd2.join_as_of(
            hechos, dimension, key="customer_id", timestamp_column="order_date"
        )
        assert resultado.count() == hechos.count()

    def test_conserva_el_hecho_aunque_no_haya_version_vigente(self, spark, dimension) -> None:
        """Un hecho anterior a la primera versión de la dimensión, o de una
        clave que no existe, no debe desaparecer: perder ingresos en silencio
        es peor que publicarlos sin atributos."""
        huerfano = spark.createDataFrame(
            [("ORD-9", "CUS-404", datetime(2026, 5, 1))],
            "order_id string, customer_id string, order_date timestamp",
        )
        resultado = scd2.join_as_of(
            huerfano, dimension, key="customer_id", timestamp_column="order_date"
        )
        assert resultado.count() == 1
        assert resultado.first().country is None

    def test_no_arrastra_las_columnas_de_vigencia(self, dimension, hechos) -> None:
        """`valid_from` y compañía son mecánica interna del historial; en una
        tabla Gold solo añadirían ruido."""
        resultado = scd2.join_as_of(
            hechos, dimension, key="customer_id", timestamp_column="order_date"
        )
        for columna in scd2.HISTORY_COLUMNS:
            assert columna not in resultado.columns


class TestConsultaDelHistorial:
    def test_recupera_la_version_vigente_en_una_fecha_dada(self, spark, destino) -> None:
        """Es para lo que existe SCD2: saber en qué país estaba el cliente
        cuando hizo aquella compra, no dónde está hoy."""
        tabla, ruta = destino
        aplicar(spark, tabla, ruta, [("CUS-1", "ES", "Madrid", T1)])

        historico = scd2.as_of(leer(spark, ruta), datetime(2026, 2, 1))
        assert historico.first().country == "MX"

        reciente = scd2.as_of(leer(spark, ruta), datetime(2026, 4, 1))
        assert reciente.first().country == "ES"

    def test_la_vista_vigente_solo_devuelve_lo_actual(self, spark, destino) -> None:
        tabla, ruta = destino
        aplicar(spark, tabla, ruta, [("CUS-1", "ES", "Madrid", T1)])

        vigente = scd2.current(leer(spark, ruta))
        assert vigente.count() == 1
        assert vigente.first().country == "ES"
