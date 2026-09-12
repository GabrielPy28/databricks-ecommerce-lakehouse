"""Construcción de las tablas Gold.

Gold es lo que ve el negocio. Un error aquí no rompe el pipeline: produce un
número equivocado en un dashboard, que es mucho peor porque nadie se entera.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from ecommerce.analytics import daily_sales

pytestmark = pytest.mark.spark

ESQUEMA = (
    "order_id string, customer_id string, order_date timestamp, status string, "
    "gross_amount decimal(12,2), discount_amount decimal(12,2), total_amount decimal(12,2)"
)


def ordenes(spark, filas):
    return spark.createDataFrame(filas, ESQUEMA)


def fila_de(df, fecha: str):
    return df.filter(f"date = '{fecha}'").first()


class TestDailySales:
    @pytest.fixture
    def resultado(self, spark):
        from datetime import datetime

        filas = [
            # Dos órdenes el mismo día, de dos clientes distintos.
            (
                "ORD-1",
                "CUS-1",
                datetime(2026, 1, 15, 10, 0),
                "delivered",
                Decimal("100.00"),
                Decimal("10.00"),
                Decimal("90.00"),
            ),
            (
                "ORD-2",
                "CUS-2",
                datetime(2026, 1, 15, 23, 59),
                "paid",
                Decimal("50.00"),
                Decimal("0.00"),
                Decimal("50.00"),
            ),
            # Un cliente que repite el mismo día: no debe contarse dos veces.
            (
                "ORD-3",
                "CUS-1",
                datetime(2026, 1, 15, 8, 0),
                "shipped",
                Decimal("30.00"),
                Decimal("0.00"),
                Decimal("30.00"),
            ),
            # Otro día.
            (
                "ORD-4",
                "CUS-3",
                datetime(2026, 1, 16, 12, 0),
                "delivered",
                Decimal("200.00"),
                Decimal("20.00"),
                Decimal("180.00"),
            ),
        ]
        return daily_sales.build(ordenes(spark, filas))

    def test_agrupa_por_dia_y_no_por_instante(self, resultado) -> None:
        """Tres órdenes en horas distintas del mismo día son una sola fila."""
        assert resultado.count() == 2

    def test_cuenta_las_ordenes_del_dia(self, resultado) -> None:
        assert fila_de(resultado, "2026-01-15").orders == 3

    def test_cuenta_clientes_distintos_y_no_ordenes(self, resultado) -> None:
        """CUS-1 compró dos veces ese día. Contarlo dos veces inflaría la base
        de clientes y falsearía cualquier métrica por cliente."""
        assert fila_de(resultado, "2026-01-15").customers == 2

    def test_suma_bruto_descuentos_y_neto(self, resultado) -> None:
        fila = fila_de(resultado, "2026-01-15")
        assert fila.gross_revenue == Decimal("180.00")
        assert fila.discounts == Decimal("10.00")
        assert fila.net_revenue == Decimal("170.00")

    def test_el_neto_cuadra_con_bruto_menos_descuentos(self, resultado) -> None:
        for fila in resultado.collect():
            assert fila.net_revenue == fila.gross_revenue - fila.discounts

    def test_el_ticket_medio_es_el_neto_entre_las_ordenes(self, resultado) -> None:
        fila = fila_de(resultado, "2026-01-16")
        assert fila.average_order_value == Decimal("180.00")


class TestEstadosQueGeneranIngreso:
    """No toda orden es una venta. Contar las canceladas inflaría los ingresos."""

    @pytest.fixture
    def resultado(self, spark):
        from datetime import datetime

        filas = [
            (
                "ORD-1",
                "CUS-1",
                datetime(2026, 1, 15, 10, 0),
                "delivered",
                Decimal("100.00"),
                Decimal("0.00"),
                Decimal("100.00"),
            ),
            (
                "ORD-2",
                "CUS-2",
                datetime(2026, 1, 15, 11, 0),
                "cancelled",
                Decimal("500.00"),
                Decimal("0.00"),
                Decimal("500.00"),
            ),
            (
                "ORD-3",
                "CUS-3",
                datetime(2026, 1, 15, 12, 0),
                "pending",
                Decimal("300.00"),
                Decimal("0.00"),
                Decimal("300.00"),
            ),
        ]
        return daily_sales.build(ordenes(spark, filas))

    def test_excluye_las_canceladas_y_las_pendientes(self, resultado) -> None:
        fila = fila_de(resultado, "2026-01-15")
        assert fila.orders == 1
        assert fila.net_revenue == Decimal("100.00")


class TestRecargaPorVentana:
    """Recalcular solo los días afectados, no la tabla entera.

    Con dos años de historia y un lote diario, rehacer `daily_sales` completa
    en cada ejecución significa releer millones de órdenes para actualizar un
    puñado de días.
    """

    def _lote(self, spark, fechas):
        from datetime import datetime

        filas = [
            (
                f"ORD-{i}",
                f"CUS-{i}",
                datetime.fromisoformat(f),
                "delivered",
                Decimal("10.00"),
                Decimal("0.00"),
                Decimal("10.00"),
            )
            for i, f in enumerate(fechas)
        ]
        return ordenes(spark, filas)

    def test_la_ventana_arranca_antes_del_dato_mas_antiguo_del_lote(self, spark) -> None:
        """El margen hacia atrás cubre los datos tardíos: un registro con fecha
        de hace tres días que llega hoy obliga a recalcular aquel día."""
        from datetime import date

        lote = self._lote(spark, ["2026-03-10T10:00", "2026-03-12T10:00"])
        desde, hasta = daily_sales.affected_range(lote, lookback_days=3)

        assert desde == date(2026, 3, 7)
        assert hasta == date(2026, 3, 12)

    def test_un_lote_sin_ordenes_no_define_ventana(self, spark) -> None:
        """Nada que recalcular es distinto de recalcularlo todo."""
        assert daily_sales.affected_range(ordenes(spark, []), lookback_days=3) is None

    def test_un_lote_con_todas_las_fechas_nulas_no_define_ventana(self, spark) -> None:
        from datetime import datetime

        filas = [
            (
                "ORD-1",
                "CUS-1",
                None,
                "delivered",
                Decimal("10.00"),
                Decimal("0.00"),
                Decimal("10.00"),
            )
        ]
        del datetime
        assert daily_sales.affected_range(ordenes(spark, filas), lookback_days=3) is None

    def test_la_reconstruccion_usa_todas_las_ordenes_del_dia_y_no_solo_las_del_lote(
        self, spark
    ) -> None:
        """El test que de verdad importa.

        Si el día se recalculara únicamente con las órdenes del lote, la
        reescritura borraría las que ya estaban y los ingresos de ese día
        caerían sin que nada fallara.
        """
        from datetime import date, datetime

        historico = self._lote(spark, ["2026-03-11T09:00", "2026-03-11T18:00", "2026-03-05T09:00"])
        del datetime

        resultado = daily_sales.rebuild_window(
            historico, desde=date(2026, 3, 10), hasta=date(2026, 3, 12)
        )

        fila = fila_de(resultado, "2026-03-11")
        assert fila.orders == 2
        assert fila.net_revenue == Decimal("20.00")

    def test_la_reconstruccion_no_devuelve_dias_fuera_de_la_ventana(self, spark) -> None:
        """Devolver días de fuera los reescribiría con datos parciales."""
        from datetime import date

        historico = self._lote(spark, ["2026-03-05T09:00", "2026-03-11T09:00"])
        resultado = daily_sales.rebuild_window(
            historico, desde=date(2026, 3, 10), hasta=date(2026, 3, 12)
        )
        assert resultado.count() == 1
        assert resultado.filter("date = '2026-03-05'").count() == 0


class TestCasosLimite:
    def test_una_fecha_nula_no_genera_un_dia_fantasma(self, spark) -> None:
        """Las órdenes con fecha corrupta van a cuarentena, pero si alguna se
        colara no debe crear una fila con fecha nula en el dashboard."""
        from datetime import datetime

        filas = [
            (
                "ORD-1",
                "CUS-1",
                None,
                "delivered",
                Decimal("100.00"),
                Decimal("0.00"),
                Decimal("100.00"),
            ),
            (
                "ORD-2",
                "CUS-2",
                datetime(2026, 1, 15, 10, 0),
                "delivered",
                Decimal("50.00"),
                Decimal("0.00"),
                Decimal("50.00"),
            ),
        ]
        resultado = daily_sales.build(ordenes(spark, filas))
        assert resultado.count() == 1
        assert resultado.filter("date IS NULL").count() == 0

    def test_sin_ordenes_devuelve_una_tabla_vacia_y_no_falla(self, spark) -> None:
        resultado = daily_sales.build(ordenes(spark, []))
        assert resultado.count() == 0
        assert "net_revenue" in resultado.columns
