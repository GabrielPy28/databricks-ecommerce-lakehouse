"""Motor de calidad dirigido por configuración.

Las reglas se declaran en `rules.yml` y un único ejecutor las aplica a todas
las tablas. La alternativa —comprobaciones escritas a mano en cada notebook—
funciona, pero duplica lógica, escala mal y hace imposible responder "¿qué
reglas tiene este proyecto?" sin leerse todo el código.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ecommerce.quality import engine, report, rules

pytestmark = pytest.mark.spark


YAML_EJEMPLO = """
silver.orders:
  - {column: order_id, rule: not_null, severity: quarantine}
  - {column: status, rule: in_set, values: [paid, delivered], severity: quarantine}
  - {column: total_amount, rule: min_value, value: 0, severity: warn}
"""


@pytest.fixture
def ordenes(spark):
    return spark.createDataFrame(
        [
            ("ORD-1", "CUS-1", "delivered", 100.0),
            ("ORD-2", "CUS-2", "paid", 50.0),
            ("ORD-3", None, "unknown_state", -10.0),
            ("ORD-4", "CUS-404", "delivered", 25.0),
        ],
        "order_id string, customer_id string, status string, total_amount double",
    )


@pytest.fixture
def clientes(spark):
    return spark.createDataFrame([("CUS-1",), ("CUS-2",)], "customer_id string")


def resultado_de(salidas, columna: str, regla: str):
    return next(s for s in salidas if s.column == columna and s.rule == regla)


class TestCargaDeReglas:
    def test_agrupa_las_reglas_por_tabla(self, tmp_path: Path) -> None:
        fichero = tmp_path / "rules.yml"
        fichero.write_text(YAML_EJEMPLO, encoding="utf-8")

        cargadas = rules.load_rules(fichero)
        assert set(cargadas) == {"silver.orders"}
        assert len(cargadas["silver.orders"]) == 3

    def test_una_regla_inexistente_falla_al_cargar_y_no_al_ejecutar(self, tmp_path: Path) -> None:
        """Un error tipográfico debe detectarse al arrancar, no a mitad del
        pipeline con media hora de cómputo ya gastada."""
        fichero = tmp_path / "rules.yml"
        fichero.write_text("silver.orders:\n  - {column: x, rule: not_nul}\n", encoding="utf-8")

        with pytest.raises(ValueError, match="not_nul"):
            rules.load_rules(fichero)

    def test_una_severidad_inexistente_falla_al_cargar(self, tmp_path: Path) -> None:
        fichero = tmp_path / "rules.yml"
        fichero.write_text(
            "silver.orders:\n  - {column: x, rule: not_null, severity: critico}\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="critico"):
            rules.load_rules(fichero)

    def test_sin_severidad_avisa_pero_no_aparta(self, tmp_path: Path) -> None:
        """El valor por defecto es el menos destructivo.

        Si el descuido de olvidar `severity` acabara en `quarantine`, un error
        de configuración retiraría datos buenos en silencio.
        """
        fichero = tmp_path / "rules.yml"
        fichero.write_text("silver.orders:\n  - {column: x, rule: not_null}\n", encoding="utf-8")
        assert rules.load_rules(fichero)["silver.orders"][0].severity == "warn"

    def test_el_fichero_del_proyecto_es_valido(self) -> None:
        """Carga el rules.yml real: un YAML mal escrito rompería el pipeline."""
        cargadas = rules.load_rules(rules.DEFAULT_RULES_PATH)
        assert cargadas
        assert all(tabla.startswith("silver.") for tabla in cargadas)


class TestReglas:
    def test_not_null_detecta_los_nulos(self, spark, ordenes) -> None:
        salidas = engine.evaluate(
            ordenes, [rules.Rule("silver.orders", "customer_id", "not_null", "quarantine", {})]
        )[1]
        assert resultado_de(salidas, "customer_id", "not_null").rows_failed == 1

    def test_in_set_detecta_los_valores_fuera_del_dominio(self, ordenes) -> None:
        regla = rules.Rule(
            "silver.orders", "status", "in_set", "quarantine", {"values": ["paid", "delivered"]}
        )
        assert (
            resultado_de(engine.evaluate(ordenes, [regla])[1], "status", "in_set").rows_failed == 1
        )

    def test_min_value_detecta_los_importes_negativos(self, ordenes) -> None:
        regla = rules.Rule("silver.orders", "total_amount", "min_value", "warn", {"value": 0})
        salida = resultado_de(engine.evaluate(ordenes, [regla])[1], "total_amount", "min_value")
        assert salida.rows_failed == 1

    def test_max_value_detecta_los_valores_excesivos(self, ordenes) -> None:
        regla = rules.Rule("silver.orders", "total_amount", "max_value", "warn", {"value": 60})
        salida = resultado_de(engine.evaluate(ordenes, [regla])[1], "total_amount", "max_value")
        # Solo ORD-1 (100.0) supera el umbral.
        assert salida.rows_failed == 1

    def test_unique_detecta_las_claves_repetidas(self, spark) -> None:
        df = spark.createDataFrame([("A",), ("A",), ("B",)], "order_id string")
        regla = rules.Rule("silver.orders", "order_id", "unique", "quarantine", {})
        salida = resultado_de(engine.evaluate(df, [regla])[1], "order_id", "unique")
        # Se marcan las dos filas implicadas: cuál de ellas sobra no se sabe.
        assert salida.rows_failed == 2

    def test_unique_puede_abarcar_varias_columnas(self, spark) -> None:
        """Lo exigen las dimensiones historificadas.

        En una dimensión SCD2 la clave de negocio aparece una vez por versión,
        así que la unicidad es de (clave, marca de tiempo). Comprobando solo la
        clave, cada cliente con historial se marcaría como duplicado y acabaría
        en cuarentena entero: el historial haría fallar su propia validación.
        """
        df = spark.createDataFrame(
            [
                ("CUS-1", "2026-01-01"),
                ("CUS-1", "2026-03-01"),
                ("CUS-2", "2026-01-01"),
            ],
            "customer_id string, updated_at string",
        )
        sin_version = rules.Rule("silver.customers", "customer_id", "unique", "fail", {})
        assert (
            resultado_de(engine.evaluate(df, [sin_version])[1], "customer_id", "unique").rows_failed
            == 2
        )

        con_version = rules.Rule(
            "silver.customers", "customer_id", "unique", "fail", {"with": ["updated_at"]}
        )
        assert (
            resultado_de(engine.evaluate(df, [con_version])[1], "customer_id", "unique").rows_failed
            == 0
        )

    def test_unique_compuesta_sigue_detectando_repeticiones_reales(self, spark) -> None:
        df = spark.createDataFrame(
            [("CUS-1", "2026-01-01"), ("CUS-1", "2026-01-01")],
            "customer_id string, updated_at string",
        )
        regla = rules.Rule(
            "silver.customers", "customer_id", "unique", "fail", {"with": ["updated_at"]}
        )
        assert (
            resultado_de(engine.evaluate(df, [regla])[1], "customer_id", "unique").rows_failed == 2
        )

    def test_foreign_key_detecta_las_referencias_rotas(self, ordenes, clientes) -> None:
        """Es la anomalía que el generador inyecta y que el tipado no detecta:
        `CUS-404` es una cadena perfectamente válida que no existe."""
        regla = rules.Rule(
            "silver.orders",
            "customer_id",
            "foreign_key",
            "quarantine",
            {"ref": "silver.customers"},
        )
        salidas = engine.evaluate(ordenes, [regla], {"silver.customers": clientes})[1]
        # CUS-404 no existe. El nulo no se cuenta: de eso se ocupa `not_null`.
        assert resultado_de(salidas, "customer_id", "foreign_key").rows_failed == 1

    def test_foreign_key_sin_referencia_falla_de_forma_explicita(self, ordenes) -> None:
        """Callar y dar la regla por superada sería mucho peor: el reporte
        diría 100 % y nadie habría comprobado nada."""
        regla = rules.Rule(
            "silver.orders", "customer_id", "foreign_key", "quarantine", {"ref": "silver.customers"}
        )
        with pytest.raises(ValueError, match=r"silver\.customers"):
            engine.evaluate(ordenes, [regla])

    def test_matches_detecta_los_correos_malformados(self, spark) -> None:
        df = spark.createDataFrame([("a@b.com",), ("sin-arroba.com",)], "email string")
        regla = rules.Rule(
            "silver.customers", "email", "matches", "warn", {"pattern": r"^[^@]+@[^@]+\.[^@]+$"}
        )
        assert resultado_de(engine.evaluate(df, [regla])[1], "email", "matches").rows_failed == 1


class TestAmbitoDeLaRegla:
    """Una regla puede acotarse a un subconjunto con `where`.

    Nace de una necesidad concreta: en una dimensión SCD2 la clave de negocio
    se repite —una fila por versión— y `unique` debe comprobarse solo sobre las
    versiones vigentes. Sin esto, historificar una dimensión haría fallar
    automáticamente su propia regla de unicidad.
    """

    @pytest.fixture
    def dimension_historificada(self, spark):
        return spark.createDataFrame(
            [
                ("CUS-1", "MX", False),
                ("CUS-1", "ES", True),
                ("CUS-2", "BR", True),
            ],
            "customer_id string, country string, is_current boolean",
        )

    def test_sin_where_la_clave_repetida_incumple(self, dimension_historificada) -> None:
        regla = rules.Rule("silver.customers", "customer_id", "unique", "fail", {})
        salida = resultado_de(
            engine.evaluate(dimension_historificada, [regla])[1], "customer_id", "unique"
        )
        assert salida.rows_failed == 2

    def test_con_where_solo_se_miran_las_filas_en_ambito(self, dimension_historificada) -> None:
        regla = rules.Rule(
            "silver.customers", "customer_id", "unique", "fail", {"where": "is_current"}
        )
        salida = resultado_de(
            engine.evaluate(dimension_historificada, [regla])[1], "customer_id", "unique"
        )
        assert salida.rows_failed == 0

    def test_las_filas_comprobadas_son_las_del_ambito(self, dimension_historificada) -> None:
        """`rows_checked` debe reflejar lo realmente comprobado.

        Si dijera 3 habiendo mirado 2, la tasa de aprobación del reporte sería
        engañosa.
        """
        regla = rules.Rule(
            "silver.customers", "customer_id", "not_null", "warn", {"where": "is_current"}
        )
        salida = resultado_de(
            engine.evaluate(dimension_historificada, [regla])[1], "customer_id", "not_null"
        )
        assert salida.rows_checked == 2

    def test_una_fila_fuera_de_ambito_no_se_aparta(self, spark) -> None:
        """La versión histórica de una dimensión no debe ir a cuarentena por
        incumplir una regla pensada para la versión vigente."""
        df = spark.createDataFrame(
            [("CUS-1", None, False), ("CUS-2", "BR", True)],
            "customer_id string, country string, is_current boolean",
        )
        regla = rules.Rule(
            "silver.customers", "country", "not_null", "quarantine", {"where": "is_current"}
        )
        marcado = engine.evaluate(df, [regla])[0]
        assert marcado.filter("size(_rule_errors) > 0").count() == 0


class TestReglasDeNivelDeTabla:
    """`row_count_delta` compara el volumen con el de la ejecución anterior.

    Es la única regla que no mira filas sino la tabla entera, y detecta lo que
    ninguna regla por fila puede ver: que **falten** datos. Una fila ausente no
    incumple nada —simplemente no está—, así que un pipeline que un día ingiere
    la décima parte de lo habitual pasa todas las validaciones y publica cifras
    silenciosamente incompletas.
    """

    @pytest.fixture
    def diez_mil_filas(self, spark):
        return spark.range(10_000).toDF("id")

    def test_se_declara_sin_columna(self, tmp_path: Path) -> None:
        fichero = tmp_path / "rules.yml"
        fichero.write_text(
            "silver.orders:\n  - {rule: row_count_delta, max_pct: 50, severity: warn}\n",
            encoding="utf-8",
        )
        regla = rules.load_rules(fichero)["silver.orders"][0]
        assert regla.column == ""
        assert regla.is_table_level

    def test_una_regla_por_fila_sin_columna_falla_al_cargar(self, tmp_path: Path) -> None:
        """Sin columna no hay nada que comprobar, y el motor la ignoraría en
        silencio: la tabla parecería validada sin estarlo."""
        fichero = tmp_path / "rules.yml"
        fichero.write_text("silver.orders:\n  - {rule: not_null}\n", encoding="utf-8")
        with pytest.raises(ValueError, match="columna"):
            rules.load_rules(fichero)

    def test_una_regla_de_tabla_con_columna_falla_al_cargar(self, tmp_path: Path) -> None:
        fichero = tmp_path / "rules.yml"
        fichero.write_text(
            "silver.orders:\n  - {column: x, rule: row_count_delta, max_pct: 10}\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="columna"):
            rules.load_rules(fichero)

    def test_sin_linea_base_no_puede_comparar_y_no_inventa_un_fallo(self, diez_mil_filas) -> None:
        """La primera ejecución no tiene con qué comparar. Marcarla como fallo
        haría que todo pipeline nuevo arrancara en rojo."""
        regla = rules.Rule("silver.orders", "", "row_count_delta", "warn", {"max_pct": 50})
        salida = engine.evaluate(diez_mil_filas, [regla])[1][0]
        assert salida.passed
        assert "línea base" in salida.detail

    def test_una_variacion_dentro_del_umbral_pasa(self, diez_mil_filas) -> None:
        regla = rules.Rule("silver.orders", "", "row_count_delta", "warn", {"max_pct": 50})
        salidas = engine.evaluate(diez_mil_filas, [regla], baselines={"silver.orders": 9_000})[1]
        assert salidas[0].passed

    def test_una_caida_brusca_se_detecta(self, diez_mil_filas) -> None:
        """La dirección peligrosa: el pipeline ingirió una fracción de lo
        habitual y ninguna regla por fila lo notaría."""
        regla = rules.Rule("silver.orders", "", "row_count_delta", "fail", {"max_pct": 20})
        salida = engine.evaluate(diez_mil_filas, [regla], baselines={"silver.orders": 100_000})[1][
            0
        ]
        assert not salida.passed
        assert "-90" in salida.detail

    def test_un_crecimiento_brusco_tambien_se_detecta(self, diez_mil_filas) -> None:
        """Un salto inesperado suele ser un lote duplicado o reprocesado."""
        regla = rules.Rule("silver.orders", "", "row_count_delta", "warn", {"max_pct": 20})
        salida = engine.evaluate(diez_mil_filas, [regla], baselines={"silver.orders": 1_000})[1][0]
        assert not salida.passed

    def test_una_regla_de_tabla_no_aparta_filas(self, diez_mil_filas) -> None:
        """No hay una fila culpable: el problema es del conjunto. Mandar la
        tabla entera a cuarentena sería absurdo."""
        regla = rules.Rule("silver.orders", "", "row_count_delta", "fail", {"max_pct": 1})
        marcado = engine.evaluate(diez_mil_filas, [regla], baselines={"silver.orders": 100_000})[0]
        assert marcado.filter("size(_rule_errors) > 0").count() == 0

    def test_una_regla_de_tabla_bloqueante_detiene_la_puerta(self, diez_mil_filas) -> None:
        regla = rules.Rule("silver.orders", "", "row_count_delta", "fail", {"max_pct": 1})
        salidas = engine.evaluate(diez_mil_filas, [regla], baselines={"silver.orders": 100_000})[1]
        assert engine.hay_fallos_bloqueantes(salidas)

    def test_el_detalle_explica_la_variacion(self, diez_mil_filas) -> None:
        """El reporte debe decir de cuánto a cuánto, no solo que falló."""
        regla = rules.Rule("silver.orders", "", "row_count_delta", "warn", {"max_pct": 20})
        salida = engine.evaluate(diez_mil_filas, [regla], baselines={"silver.orders": 5_000})[1][0]
        assert "5,000" in salida.detail and "10,000" in salida.detail


class TestResultados:
    def test_registra_cuantas_filas_se_comprobaron_y_cuantas_fallaron(self, ordenes) -> None:
        regla = rules.Rule("silver.orders", "customer_id", "not_null", "quarantine", {})
        salida = resultado_de(engine.evaluate(ordenes, [regla])[1], "customer_id", "not_null")
        assert salida.rows_checked == 4
        assert salida.rows_failed == 1

    def test_calcula_la_tasa_de_aprobacion(self, ordenes) -> None:
        regla = rules.Rule("silver.orders", "customer_id", "not_null", "quarantine", {})
        salida = resultado_de(engine.evaluate(ordenes, [regla])[1], "customer_id", "not_null")
        assert salida.pass_rate == pytest.approx(75.0)

    def test_una_tabla_vacia_no_divide_entre_cero(self, spark) -> None:
        df = spark.createDataFrame([], "order_id string")
        regla = rules.Rule("silver.orders", "order_id", "not_null", "quarantine", {})
        salida = resultado_de(engine.evaluate(df, [regla])[1], "order_id", "not_null")
        assert salida.pass_rate == 100.0


class TestSeveridad:
    def test_quarantine_marca_la_fila_para_apartarla(self, ordenes) -> None:
        regla = rules.Rule("silver.orders", "customer_id", "not_null", "quarantine", {})
        marcado = engine.evaluate(ordenes, [regla])[0]
        fallidas = marcado.filter("size(_rule_errors) > 0")
        assert fallidas.count() == 1
        assert fallidas.first()._rule_errors == ["customer_id:not_null"]

    def test_warn_deja_pasar_la_fila(self, ordenes) -> None:
        """Un aviso informa, no retira datos. Es la diferencia entre "esto
        merece una mirada" y "esto no se puede usar"."""
        regla = rules.Rule("silver.orders", "total_amount", "min_value", "warn", {"value": 0})
        marcado = engine.evaluate(ordenes, [regla])[0]
        assert marcado.filter("size(_rule_errors) > 0").count() == 0

    def test_fail_se_senala_para_que_la_puerta_lo_detenga(self, ordenes) -> None:
        regla = rules.Rule("silver.orders", "customer_id", "not_null", "fail", {})
        salidas = engine.evaluate(ordenes, [regla])[1]
        assert engine.hay_fallos_bloqueantes(salidas)

    def test_sin_incumplimientos_la_puerta_no_bloquea(self, ordenes) -> None:
        regla = rules.Rule("silver.orders", "order_id", "not_null", "fail", {})
        salidas = engine.evaluate(ordenes, [regla])[1]
        assert not engine.hay_fallos_bloqueantes(salidas)


class TestReporte:
    @pytest.fixture
    def salidas(self, ordenes, clientes):
        conjunto = [
            rules.Rule("silver.orders", "order_id", "not_null", "quarantine", {}),
            rules.Rule("silver.orders", "customer_id", "not_null", "quarantine", {}),
            rules.Rule(
                "silver.orders",
                "customer_id",
                "foreign_key",
                "quarantine",
                {"ref": "silver.customers"},
            ),
            rules.Rule("silver.orders", "total_amount", "min_value", "warn", {"value": 0}),
        ]
        return engine.evaluate(ordenes, conjunto, {"silver.customers": clientes})[1]

    def test_muestra_la_tabla_y_las_filas_procesadas(self, salidas) -> None:
        texto = report.format_report(salidas)
        assert "silver.orders" in texto
        assert "4" in texto

    def test_distingue_las_reglas_superadas_de_las_incumplidas(self, salidas) -> None:
        texto = report.format_report(salidas)
        lineas = [linea for linea in texto.splitlines() if "not_null" in linea]
        assert any(linea.lstrip().startswith(report.MARCA_OK) for linea in lineas)
        assert any(linea.lstrip().startswith(report.MARCA_FALLO) for linea in lineas)

    def test_resume_cuantas_filas_quedan_apartadas(self, salidas) -> None:
        """Es el número que se cita en una entrevista, así que debe salir del
        reporte y no de una cuenta hecha a mano."""
        texto = report.format_report(salidas)
        assert "Cuarentena" in texto

    def test_las_filas_para_ops_llevan_la_ejecucion_y_el_momento(self, salidas) -> None:
        filas = report.to_rows(salidas, run_id="run-1", checked_at="2026-09-06T00:00:00")
        assert len(filas) == len(salidas)
        assert filas[0]["run_id"] == "run-1"
        assert filas[0]["checked_at"] == "2026-09-06T00:00:00"
        assert set(filas[0]) >= {
            "run_id",
            "checked_at",
            "table",
            "column",
            "rule",
            "severity",
            "rows_checked",
            "rows_failed",
            "pass_rate",
        }
