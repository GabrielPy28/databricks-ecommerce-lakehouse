"""Variables y etiqueta del modelo de churn.

El riesgo de un modelo de churn no es que prediga mal: es que prediga
**demasiado bien** porque la etiqueta se coló entre las variables. Definir churn
como "sin compras en 90 días" y luego alimentar el modelo con
`days_since_last_order` produce un clasificador perfecto y completamente
inútil.

Por eso el test más importante de este fichero no comprueba una cifra: comprueba
que añadir datos posteriores al corte **no cambie ni una variable**.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from ecommerce.ml import features

pytestmark = pytest.mark.spark

ESQUEMA_ORDENES = (
    "order_id string, customer_id string, order_date timestamp, status string, "
    "total_amount decimal(12,2)"
)
ESQUEMA_DEVOLUCIONES = "return_id string, order_id string, quantity int"

CORTE = date(2026, 6, 1)


def orden(oid: str, cliente: str, cuando: datetime, importe: str = "100.00", estado="delivered"):
    return (oid, cliente, cuando, estado, Decimal(importe))


def fila(df, customer_id: str):
    return df.filter(f"customer_id = '{customer_id}'").first()


@pytest.fixture
def historico(spark):
    """Tres clientes con comportamientos deliberadamente distintos."""
    return spark.createDataFrame(
        [
            # Fiel y reciente: cuatro compras, la última justo antes del corte.
            orden("O1", "CUS-1", datetime(2025, 6, 1), "100.00"),
            orden("O2", "CUS-1", datetime(2026, 1, 1), "200.00"),
            orden("O3", "CUS-1", datetime(2026, 5, 10), "300.00"),
            orden("O4", "CUS-1", datetime(2026, 5, 25), "400.00"),
            # Antiguo y dormido: una compra hace más de un año.
            orden("O5", "CUS-2", datetime(2025, 3, 1), "50.00"),
            # Cancelada: no cuenta como actividad comercial.
            orden("O6", "CUS-3", datetime(2026, 5, 20), "999.00", "cancelled"),
            orden("O7", "CUS-3", datetime(2026, 4, 1), "80.00"),
        ],
        ESQUEMA_ORDENES,
    )


@pytest.fixture
def sin_devoluciones(spark):
    return spark.createDataFrame([], ESQUEMA_DEVOLUCIONES)


class TestAusenciaDeFuga:
    """Ninguna variable puede depender de lo que pasó después del corte.

    Es la propiedad que separa un modelo de una tautología, y la única que se
    puede comprobar de forma tajante: los mismos datos hasta el corte deben
    producir exactamente las mismas variables, haya lo que haya después.
    """

    def test_los_datos_posteriores_al_corte_no_alteran_ninguna_variable(
        self, spark, historico, sin_devoluciones
    ) -> None:
        antes = features.build_features(historico, sin_devoluciones, cutoff=CORTE)

        # Se añade actividad intensa después del corte para los tres clientes.
        posteriores = spark.createDataFrame(
            [
                orden("P1", "CUS-1", datetime(2026, 7, 1), "5000.00"),
                orden("P2", "CUS-2", datetime(2026, 8, 1), "5000.00"),
                orden("P3", "CUS-3", datetime(2026, 8, 15), "5000.00"),
            ],
            ESQUEMA_ORDENES,
        )
        despues = features.build_features(
            historico.union(posteriores), sin_devoluciones, cutoff=CORTE
        )

        assert antes.collect() == despues.collect()

    def test_un_cliente_que_solo_compra_despues_del_corte_no_entra(
        self, spark, historico, sin_devoluciones
    ) -> None:
        """No se puede predecir la fuga de quien todavía no era cliente."""
        futuro = spark.createDataFrame(
            [orden("P9", "CUS-NUEVO", datetime(2026, 7, 1))], ESQUEMA_ORDENES
        )
        resultado = features.build_features(historico.union(futuro), sin_devoluciones, cutoff=CORTE)
        assert fila(resultado, "CUS-NUEVO") is None


class TestVariables:
    @pytest.fixture
    def resultado(self, historico, sin_devoluciones):
        return features.build_features(historico, sin_devoluciones, cutoff=CORTE)

    def test_la_recencia_se_mide_hasta_el_corte(self, resultado) -> None:
        # CUS-1 compró el 25 de mayo; el corte es el 1 de junio.
        assert fila(resultado, "CUS-1").recency_days == 7

    def test_cuenta_las_ordenes_con_ingreso_reconocido(self, resultado) -> None:
        """La cancelada de CUS-3 no es actividad comercial."""
        assert fila(resultado, "CUS-1").frequency == 4
        assert fila(resultado, "CUS-3").frequency == 1

    def test_acumula_el_gasto(self, resultado) -> None:
        assert fila(resultado, "CUS-1").monetary == Decimal("1000.00")

    def test_las_ventanas_recientes_solo_miran_su_periodo(self, resultado) -> None:
        # De CUS-1, las de mayo caen en ambas ventanas; la de enero queda a 151
        # días del corte y no entra en la de 90.
        c1 = fila(resultado, "CUS-1")
        assert c1.orders_last_30d == 2
        assert c1.orders_last_90d == 2
        assert c1.spend_last_90d == Decimal("700.00")

    def test_la_antiguedad_se_mide_desde_la_primera_compra(self, resultado) -> None:
        # CUS-2 compró el 1 de marzo de 2025.
        assert fila(resultado, "CUS-2").tenure_days == 457

    def test_con_una_sola_compra_el_intervalo_medio_es_la_antiguedad(self, resultado) -> None:
        """No hay intervalo que medir. Poner cero diría "compra sin parar", que
        es lo contrario de la verdad; la antigüedad refleja que lleva todo ese
        tiempo sin repetir."""
        c2 = fila(resultado, "CUS-2")
        assert c2.frequency == 1
        assert c2.avg_days_between_orders == c2.tenure_days

    def test_no_deja_variables_nulas(self, resultado) -> None:
        """Un nulo rompe el ensamblado de vectores de MLlib, y rellenarlo más
        tarde con un valor arbitrario esconde el problema."""
        for f in resultado.collect():
            for columna in features.FEATURE_COLUMNS:
                assert f[columna] is not None, columna


class TestTasaDeDevolucion:
    def test_solo_cuenta_devoluciones_anteriores_al_corte(self, spark, historico) -> None:
        devoluciones = spark.createDataFrame(
            [
                ("R1", "O3", 1),  # 10 de mayo, antes del corte
                ("R2", "P1", 1),  # de una orden posterior: no debe contar
            ],
            ESQUEMA_DEVOLUCIONES,
        )
        resultado = features.build_features(historico, devoluciones, cutoff=CORTE)
        # CUS-1 tiene 4 órdenes y 1 devuelta antes del corte.
        assert fila(resultado, "CUS-1").return_rate == pytest.approx(25.0)

    def test_sin_devoluciones_la_tasa_es_cero_y_no_nula(self, historico, sin_devoluciones) -> None:
        resultado = features.build_features(historico, sin_devoluciones, cutoff=CORTE)
        assert fila(resultado, "CUS-2").return_rate == 0.0


class TestEtiqueta:
    def test_es_uno_cuando_no_compra_en_el_horizonte(self, spark, historico) -> None:
        futuro = spark.createDataFrame(
            [orden("P1", "CUS-1", datetime(2026, 6, 20))], ESQUEMA_ORDENES
        )
        etiquetas = features.build_label(historico.union(futuro), cutoff=CORTE, horizon_days=90)
        assert fila(etiquetas, "CUS-1").churned == 0
        assert fila(etiquetas, "CUS-2").churned == 1

    def test_una_compra_posterior_al_horizonte_no_cuenta_como_retencion(
        self, spark, historico
    ) -> None:
        """Si volviera dentro de un año seguiría siendo fuga en este horizonte.
        Contarla haría que la etiqueta dependiera de cuántos datos haya después,
        no del comportamiento del cliente."""
        muy_tarde = spark.createDataFrame(
            [orden("P1", "CUS-1", datetime(2027, 1, 1))], ESQUEMA_ORDENES
        )
        etiquetas = features.build_label(historico.union(muy_tarde), cutoff=CORTE, horizon_days=90)
        assert fila(etiquetas, "CUS-1").churned == 1

    def test_una_orden_cancelada_en_el_horizonte_no_retiene(self, spark, historico) -> None:
        cancelada = spark.createDataFrame(
            [orden("P1", "CUS-2", datetime(2026, 6, 20), "100.00", "cancelled")],
            ESQUEMA_ORDENES,
        )
        etiquetas = features.build_label(historico.union(cancelada), cutoff=CORTE, horizon_days=90)
        assert fila(etiquetas, "CUS-2").churned == 1


class TestConjuntoDeEntrenamiento:
    def test_une_variables_y_etiqueta_sin_perder_clientes(
        self, spark, historico, sin_devoluciones
    ) -> None:
        conjunto = features.build_training_set(
            historico, sin_devoluciones, cutoff=CORTE, horizon_days=90
        )
        variables = features.build_features(historico, sin_devoluciones, cutoff=CORTE)
        assert conjunto.count() == variables.count()
        assert "churned" in conjunto.columns

    def test_toda_fila_tiene_etiqueta(self, historico, sin_devoluciones) -> None:
        """Un cliente activo antes del corte que no aparece después es fuga, no
        un nulo. Dejarlo sin etiqueta descartaría justo los casos positivos."""
        conjunto = features.build_training_set(
            historico, sin_devoluciones, cutoff=CORTE, horizon_days=90
        )
        assert conjunto.filter("churned IS NULL").count() == 0
