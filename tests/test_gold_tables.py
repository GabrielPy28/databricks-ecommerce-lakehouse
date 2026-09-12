"""Las tablas Gold que alimentan el dashboard.

Gold es lo que ve el negocio. Un error aquí no rompe el pipeline: produce un
número equivocado en un panel, que es peor porque nadie se entera.

Las cifras de estos tests están calculadas a mano a propósito. Comprobar que el
código coincide consigo mismo no demuestra nada.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from ecommerce.analytics import (
    category_performance,
    customer_ltv,
    customer_segments,
    marketing_funnel,
    product_performance,
)

pytestmark = pytest.mark.spark


ESQUEMA_ORDENES = (
    "order_id string, customer_id string, order_date timestamp, status string, "
    "gross_amount decimal(12,2), discount_amount decimal(12,2), total_amount decimal(12,2)"
)
ESQUEMA_LINEAS = (
    "order_item_id string, order_id string, product_id string, quantity int, "
    "unit_price decimal(10,2), discount_amount decimal(10,2), line_total decimal(12,2)"
)
ESQUEMA_PRODUCTOS_SCD2 = (
    "product_id string, product_name string, category string, cost decimal(10,2), "
    "valid_from timestamp, valid_to timestamp, is_current boolean"
)
ESQUEMA_CLIENTES_SCD2 = (
    "customer_id string, country string, valid_from timestamp, valid_to timestamp, "
    "is_current boolean"
)
ESQUEMA_DEVOLUCIONES = (
    "return_id string, order_item_id string, product_id string, quantity int, "
    "refund_amount decimal(12,2), return_date timestamp"
)
ESQUEMA_EVENTOS = (
    "event_id string, event_timestamp timestamp, session_id string, "
    "event_type string, utm_source string"
)

ENERO = datetime(2026, 1, 15, 10, 0)
MARZO = datetime(2026, 3, 15, 10, 0)


def fila(df, **filtros):
    condicion = " AND ".join(f"{k} = '{v}'" for k, v in filtros.items())
    return df.filter(condicion).first()


class TestProductPerformance:
    @pytest.fixture
    def resultado(self, spark):
        ordenes = spark.createDataFrame(
            [
                (
                    "ORD-1",
                    "CUS-1",
                    ENERO,
                    "delivered",
                    *[Decimal("200.00")] * 1,
                    Decimal("0.00"),
                    Decimal("200.00"),
                ),
                (
                    "ORD-2",
                    "CUS-2",
                    MARZO,
                    "delivered",
                    Decimal("300.00"),
                    Decimal("0.00"),
                    Decimal("300.00"),
                ),
                # Cancelada: no es ingreso y no debe contar unidades vendidas.
                (
                    "ORD-3",
                    "CUS-3",
                    MARZO,
                    "cancelled",
                    Decimal("999.00"),
                    Decimal("0.00"),
                    Decimal("999.00"),
                ),
            ],
            ESQUEMA_ORDENES,
        )
        lineas = spark.createDataFrame(
            [
                (
                    "ITM-1",
                    "ORD-1",
                    "PRD-1",
                    2,
                    Decimal("100.00"),
                    Decimal("0.00"),
                    Decimal("200.00"),
                ),
                (
                    "ITM-2",
                    "ORD-2",
                    "PRD-1",
                    3,
                    Decimal("100.00"),
                    Decimal("0.00"),
                    Decimal("300.00"),
                ),
                (
                    "ITM-3",
                    "ORD-3",
                    "PRD-1",
                    9,
                    Decimal("111.00"),
                    Decimal("0.00"),
                    Decimal("999.00"),
                ),
            ],
            ESQUEMA_LINEAS,
        )
        # El coste del producto cambió en febrero: 40 antes, 60 después.
        productos = spark.createDataFrame(
            [
                (
                    "PRD-1",
                    "Widget",
                    "Home",
                    Decimal("40.00"),
                    datetime(2026, 1, 1),
                    datetime(2026, 2, 1),
                    False,
                ),
                ("PRD-1", "Widget", "Home", Decimal("60.00"), datetime(2026, 2, 1), None, True),
            ],
            ESQUEMA_PRODUCTOS_SCD2,
        )
        devoluciones = spark.createDataFrame(
            [("RET-1", "ITM-2", "PRD-1", 1, Decimal("100.00"), MARZO)],
            ESQUEMA_DEVOLUCIONES,
        )
        return product_performance.build(lineas, ordenes, productos, devoluciones)

    def test_suma_unidades_e_ingresos_de_ordenes_validas(self, resultado) -> None:
        p = fila(resultado, product_id="PRD-1")
        assert p.units_sold == 5  # 2 + 3; las 9 de la cancelada no cuentan
        assert p.revenue == Decimal("500.00")

    def test_usa_el_coste_vigente_en_la_fecha_de_la_venta(self, resultado) -> None:
        """Aquí rinde el historial SCD2.

        Con el coste actual (60) para todo, el coste sería 5 x 60 = 300. Con el
        de cada fecha: 2 x 40 (enero) + 3 x 60 (marzo) = 260. La diferencia es
        un 15 % de margen inventado.
        """
        p = fila(resultado, product_id="PRD-1")
        assert p.cost == Decimal("260.00")
        assert p.profit == Decimal("240.00")

    def test_calcula_la_tasa_de_devolucion_sobre_unidades(self, resultado) -> None:
        p = fila(resultado, product_id="PRD-1")
        assert p.returned_units == 1
        assert p.return_rate == pytest.approx(20.0)  # 1 de 5

    def test_un_producto_sin_devoluciones_tiene_tasa_cero_y_no_nula(self, spark) -> None:
        """Un nulo en el dashboard se lee como 'no se sabe'; aquí sí se sabe."""
        ordenes = spark.createDataFrame(
            [
                (
                    "ORD-1",
                    "CUS-1",
                    ENERO,
                    "delivered",
                    Decimal("50.00"),
                    Decimal("0.00"),
                    Decimal("50.00"),
                )
            ],
            ESQUEMA_ORDENES,
        )
        lineas = spark.createDataFrame(
            [("ITM-1", "ORD-1", "PRD-9", 1, Decimal("50.00"), Decimal("0.00"), Decimal("50.00"))],
            ESQUEMA_LINEAS,
        )
        productos = spark.createDataFrame(
            [("PRD-9", "Otro", "Home", Decimal("10.00"), datetime(2026, 1, 1), None, True)],
            ESQUEMA_PRODUCTOS_SCD2,
        )
        vacias = spark.createDataFrame([], ESQUEMA_DEVOLUCIONES)

        p = fila(product_performance.build(lineas, ordenes, productos, vacias), product_id="PRD-9")
        assert p.returned_units == 0
        assert p.return_rate == 0.0


class TestCategoryPerformance:
    @pytest.fixture
    def resultado(self, spark):
        ordenes = spark.createDataFrame(
            [
                (
                    "ORD-1",
                    "CUS-1",
                    ENERO,
                    "delivered",
                    Decimal("200.00"),
                    Decimal("0.00"),
                    Decimal("200.00"),
                ),
                (
                    "ORD-2",
                    "CUS-2",
                    datetime(2026, 1, 28, 9, 0),
                    "paid",
                    Decimal("100.00"),
                    Decimal("0.00"),
                    Decimal("100.00"),
                ),
                (
                    "ORD-3",
                    "CUS-3",
                    MARZO,
                    "delivered",
                    Decimal("300.00"),
                    Decimal("0.00"),
                    Decimal("300.00"),
                ),
            ],
            ESQUEMA_ORDENES,
        )
        lineas = spark.createDataFrame(
            [
                (
                    "ITM-1",
                    "ORD-1",
                    "PRD-1",
                    2,
                    Decimal("100.00"),
                    Decimal("0.00"),
                    Decimal("200.00"),
                ),
                (
                    "ITM-2",
                    "ORD-2",
                    "PRD-2",
                    1,
                    Decimal("100.00"),
                    Decimal("0.00"),
                    Decimal("100.00"),
                ),
                (
                    "ITM-3",
                    "ORD-3",
                    "PRD-1",
                    3,
                    Decimal("100.00"),
                    Decimal("0.00"),
                    Decimal("300.00"),
                ),
            ],
            ESQUEMA_LINEAS,
        )
        productos = spark.createDataFrame(
            [
                ("PRD-1", "Widget", "Home", Decimal("50.00"), datetime(2026, 1, 1), None, True),
                (
                    "PRD-2",
                    "Gadget",
                    "Electronics",
                    Decimal("20.00"),
                    datetime(2026, 1, 1),
                    None,
                    True,
                ),
            ],
            ESQUEMA_PRODUCTOS_SCD2,
        )
        return category_performance.build(lineas, ordenes, productos)

    def test_agrupa_por_categoria_y_mes(self, resultado) -> None:
        assert resultado.count() == 3  # Home/enero, Electronics/enero, Home/marzo
        assert fila(resultado, category="Home", month="2026-01-01").revenue == Decimal("200.00")
        assert fila(resultado, category="Home", month="2026-03-01").revenue == Decimal("300.00")

    def test_el_mes_es_el_primer_dia_y_no_una_cadena(self, resultado) -> None:
        """Un mes como texto ordena mal en cuanto cambia el año, y no se puede
        filtrar por rango en el dashboard."""
        assert isinstance(resultado.first().month, date)

    def test_cuenta_ordenes_distintas_y_no_lineas(self, resultado) -> None:
        assert fila(resultado, category="Home", month="2026-01-01").orders == 1


class TestCustomerLifetimeValue:
    @pytest.fixture
    def resultado(self, spark):
        ordenes = spark.createDataFrame(
            [
                (
                    "ORD-1",
                    "CUS-1",
                    datetime(2026, 1, 10),
                    "delivered",
                    Decimal("100.00"),
                    Decimal("0.00"),
                    Decimal("100.00"),
                ),
                (
                    "ORD-2",
                    "CUS-1",
                    datetime(2026, 3, 10),
                    "delivered",
                    Decimal("300.00"),
                    Decimal("0.00"),
                    Decimal("300.00"),
                ),
                (
                    "ORD-3",
                    "CUS-1",
                    datetime(2026, 2, 10),
                    "cancelled",
                    Decimal("999.00"),
                    Decimal("0.00"),
                    Decimal("999.00"),
                ),
                (
                    "ORD-4",
                    "CUS-2",
                    datetime(2026, 2, 1),
                    "paid",
                    Decimal("50.00"),
                    Decimal("0.00"),
                    Decimal("50.00"),
                ),
            ],
            ESQUEMA_ORDENES,
        )
        clientes = spark.createDataFrame(
            [
                ("CUS-1", "MX", datetime(2026, 1, 1), None, True),
                ("CUS-2", "ES", datetime(2026, 1, 1), None, True),
            ],
            ESQUEMA_CLIENTES_SCD2,
        )
        return customer_ltv.build(ordenes, clientes)

    def test_acumula_gasto_y_ordenes_validas(self, resultado) -> None:
        c = fila(resultado, customer_id="CUS-1")
        assert c.orders == 2  # la cancelada no cuenta
        assert c.total_spend == Decimal("400.00")

    def test_el_ticket_medio_es_el_gasto_entre_las_ordenes(self, resultado) -> None:
        assert fila(resultado, customer_id="CUS-1").average_order_value == Decimal("200.00")

    def test_registra_la_primera_y_la_ultima_compra(self, resultado) -> None:
        c = fila(resultado, customer_id="CUS-1")
        assert c.first_order == date(2026, 1, 10)
        assert c.last_order == date(2026, 3, 10)

    def test_el_valor_de_vida_es_el_gasto_historico(self, resultado) -> None:
        """Se nombra y se calcula como lo que es: gasto acumulado, no una
        predicción. Llamar "valor de vida" a un modelo inexistente sería la
        clase de cifra que nadie puede defender en una reunión.
        """
        c = fila(resultado, customer_id="CUS-1")
        assert c.lifetime_value == c.total_spend

    def test_incluye_el_pais_vigente_del_cliente(self, resultado) -> None:
        assert fila(resultado, customer_id="CUS-2").country == "ES"


class TestCustomerSegments:
    @pytest.fixture
    def resultado(self, spark):
        # Cuatro clientes con perfiles deliberadamente distintos.
        base = [
            ("CUS-1", "MX", 10, Decimal("5000.00"), date(2026, 9, 1)),  # reciente, frecuente, gasta
            ("CUS-2", "ES", 1, Decimal("50.00"), date(2026, 8, 30)),  # reciente, nuevo
            (
                "CUS-3",
                "BR",
                8,
                Decimal("4000.00"),
                date(2026, 2, 1),
            ),  # fue bueno, lleva meses sin comprar
            ("CUS-4", "US", 1, Decimal("30.00"), date(2025, 10, 1)),  # perdido
        ]
        ltv = spark.createDataFrame(
            [(c, p, o, g, f) for c, p, o, g, f in base],
            "customer_id string, country string, orders int, "
            "total_spend decimal(12,2), last_order date",
        )
        return customer_segments.build(ltv, reference_date=date(2026, 9, 1))

    def test_calcula_la_recencia_en_dias_desde_la_fecha_de_referencia(self, resultado) -> None:
        """La referencia se recibe, no se toma de `today()`: con la fecha del
        sistema, la segmentación cambiaría cada día y dejaría de ser
        reproducible."""
        assert fila(resultado, customer_id="CUS-1").recency_days == 0
        assert fila(resultado, customer_id="CUS-3").recency_days == 212

    def test_asigna_un_segmento_a_cada_cliente(self, resultado) -> None:
        assert resultado.count() == 4
        assert resultado.filter("segment IS NULL").count() == 0

    def test_el_mejor_cliente_y_el_perdido_no_caen_en_el_mismo_segmento(self, resultado) -> None:
        mejor = fila(resultado, customer_id="CUS-1").segment
        perdido = fila(resultado, customer_id="CUS-4").segment
        assert mejor != perdido

    def test_las_puntuaciones_estan_en_el_rango_declarado(self, resultado) -> None:
        for f in resultado.collect():
            for puntuacion in (f.r_score, f.f_score, f.m_score):
                assert 1 <= puntuacion <= customer_segments.QUINTILES

    def test_mas_reciente_obtiene_mejor_puntuacion_de_recencia(self, resultado) -> None:
        """Recencia invertida: menos días es mejor. Olvidarlo produce una
        segmentación que llama campeones a los clientes perdidos."""
        assert (
            fila(resultado, customer_id="CUS-1").r_score
            > fila(resultado, customer_id="CUS-4").r_score
        )


class TestMarketingFunnel:
    @pytest.fixture
    def resultado(self, spark):
        dia = datetime(2026, 5, 10, 12, 0)
        eventos = [
            ("E1", dia, "S1", "page_view", "organic"),
            ("E2", dia, "S1", "add_to_cart", "organic"),
            ("E3", dia, "S2", "page_view", "organic"),
            ("E4", dia, "S2", "add_to_cart", "organic"),
            ("E5", dia, "S2", "checkout_start", "organic"),
            ("E6", dia, "S2", "purchase", "organic"),
            ("E7", dia, "S3", "page_view", "paid_search"),
            ("E8", dia, "S4", "page_view", "organic"),
        ]
        return marketing_funnel.build(spark.createDataFrame(eventos, ESQUEMA_EVENTOS))

    def test_agrupa_por_dia_y_canal(self, resultado) -> None:
        assert resultado.count() == 2
        assert fila(resultado, date="2026-05-10", channel="organic").page_views == 3
        assert fila(resultado, date="2026-05-10", channel="paid_search").page_views == 1

    def test_cuenta_sesiones_y_no_eventos(self, resultado) -> None:
        """Tres vistas de página de la misma sesión son una visita, no tres.

        Contando eventos, la tasa de conversión saldría dividida por el número
        de páginas que el visitante mirase.
        """
        organico = fila(resultado, date="2026-05-10", channel="organic")
        assert organico.page_views == 3  # S1, S2, S4
        assert organico.add_to_carts == 2  # S1, S2
        assert organico.checkouts == 1  # S2
        assert organico.purchases == 1  # S2

    def test_el_embudo_no_se_ensancha(self, resultado) -> None:
        for f in resultado.collect():
            assert f.page_views >= f.add_to_carts >= f.checkouts >= f.purchases

    def test_calcula_la_conversion_sobre_las_visitas(self, resultado) -> None:
        organico = fila(resultado, date="2026-05-10", channel="organic")
        assert organico.conversion_rate == pytest.approx(33.33, abs=0.01)  # 1 de 3

    def test_un_canal_sin_compras_convierte_cero_y_no_nulo(self, resultado) -> None:
        assert fila(resultado, date="2026-05-10", channel="paid_search").conversion_rate == 0.0
