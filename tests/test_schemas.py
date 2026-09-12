"""El contrato de datos.

Estos tests son deliberadamente estrictos. El documento de idea original tenía
huecos —métricas de Gold sin columna de origen— y estos tests existen para que
no vuelvan a abrirse sin que nadie se entere.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from ecommerce import schemas
from ecommerce.schemas import metadata


class TestRegistroDeEntidades:
    def test_las_ocho_entidades_estan_registradas(self) -> None:
        assert set(schemas.ALL_SCHEMAS) == {
            "customers",
            "products",
            "orders",
            "order_items",
            "payments",
            "reviews",
            "returns",
            "web_events",
        }

    def test_toda_entidad_declara_su_clave_primaria(self) -> None:
        for entidad, esquema in schemas.ALL_SCHEMAS.items():
            clave = schemas.PRIMARY_KEYS[entidad]
            assert clave in esquema.names, f"{entidad}: la clave '{clave}' no está en el esquema"

    def test_toda_entidad_declara_como_desempatar_versiones(self) -> None:
        """Silver hace MERGE y necesita saber qué versión de una fila es la buena.

        Las entidades que mutan desempatan por `updated_at`; las inmutables
        —un evento de navegación ya ocurrido, una reseña ya escrita— por su
        propia clave, lo que las convierte en solo-inserción.
        """
        for entidad, esquema in schemas.ALL_SCHEMAS.items():
            columna = schemas.ORDER_BY[entidad]
            assert columna in esquema.names, f"{entidad}: '{columna}' no está en el esquema"


class TestEntidadesQueMutan:
    def test_payments_desempata_por_updated_at(self) -> None:
        """Un pago cambia de estado: pending -> completed -> refunded."""
        assert schemas.ORDER_BY["payments"] == "updated_at"
        assert "updated_at" in schemas.PAYMENTS.names

    @pytest.mark.parametrize("entidad", ["reviews", "returns", "web_events"])
    def test_las_entidades_inmutables_desempatan_por_su_clave(self, entidad: str) -> None:
        """Un hecho ya ocurrido no cambia. Darle `updated_at` sugeriría lo
        contrario e invitaría a escribir lógica de actualización que sobra."""
        assert schemas.ORDER_BY[entidad] == schemas.PRIMARY_KEYS[entidad]


class TestHuecosCerrados:
    """Cada aserción corresponde a un campo que faltaba en el documento original."""

    def test_orders_tiene_updated_at(self) -> None:
        """Sin `updated_at` no hay MERGE con desempate ni datos tardíos.

        Es el campo más importante del proyecto: sin él, el procesamiento
        incremental degenera en un append y deja de ser idempotente.
        """
        assert "updated_at" in schemas.ORDERS.names

    def test_orders_tiene_los_importes_que_gold_necesita(self) -> None:
        """`daily_sales` pide ingreso bruto, descuentos y neto."""
        for columna in ("gross_amount", "discount_amount", "total_amount"):
            assert columna in schemas.ORDERS.names

    def test_products_tiene_nombre_y_coste(self) -> None:
        """`product_performance` pide product_name y margen (necesita cost)."""
        assert "product_name" in schemas.PRODUCTS.names
        assert "cost" in schemas.PRODUCTS.names

    def test_order_items_tiene_descuento_de_linea(self) -> None:
        """El descuento del pedido se agrega desde las líneas."""
        assert "discount_amount" in schemas.ORDER_ITEMS.names

    def test_returns_permite_calcular_la_tasa_de_devolucion(self) -> None:
        """`product_performance.return_rate` la necesita, y en el documento
        original `returns` se mencionaba como fuente pero nunca se definía."""
        for columna in ("product_id", "order_item_id", "quantity", "refund_amount"):
            assert columna in schemas.RETURNS.names

    def test_web_events_permite_construir_el_embudo(self) -> None:
        """`marketing_funnel` necesita tipo de evento, sesión y canal."""
        for columna in ("event_type", "session_id", "utm_source"):
            assert columna in schemas.WEB_EVENTS.names


class TestSesionesAnonimas:
    def test_un_evento_web_puede_no_tener_cliente(self) -> None:
        """La mayor parte del tráfico de un e-commerce no ha iniciado sesión.

        Exigir `customer_id` obligaría a inventar clientes falsos y falsearía
        por completo la parte alta del embudo.
        """
        assert schemas.WEB_EVENTS.field("customer_id").nullable

    def test_un_evento_web_puede_no_referirse_a_un_producto(self) -> None:
        """Ver la portada es un evento sin producto asociado."""
        assert schemas.WEB_EVENTS.field("product_id").nullable


class TestTiposDelContrato:
    def test_el_dinero_es_decimal_y_nunca_coma_flotante(self) -> None:
        """`float` acumula error de redondeo en sumas de importes.

        Un total de ventas que no cuadra con la suma de sus líneas por 0,01 es
        un fallo que cuesta horas encontrar y destruye la confianza en el
        pipeline.
        """
        columnas_monetarias = {
            schemas.PRODUCTS: ("price", "cost"),
            schemas.ORDERS: ("gross_amount", "discount_amount", "total_amount"),
            schemas.ORDER_ITEMS: ("unit_price", "discount_amount", "line_total"),
        }
        for esquema, columnas in columnas_monetarias.items():
            for columna in columnas:
                tipo = esquema.field(columna).type
                assert pa.types.is_decimal(tipo), f"{columna} es {tipo}, debería ser decimal"

    def test_las_marcas_de_tiempo_son_timestamp(self) -> None:
        assert pa.types.is_timestamp(schemas.ORDERS.field("order_date").type)
        assert pa.types.is_timestamp(schemas.ORDERS.field("updated_at").type)

    def test_la_cantidad_es_entera(self) -> None:
        assert pa.types.is_integer(schemas.ORDER_ITEMS.field("quantity").type)


class TestEsquemaDeAterrizaje:
    """Los ficheros crudos llegan con todo como texto.

    Es lo que permite inyectar suciedad: una columna Parquet tipada como
    timestamp no admite una fecha inválida, así que un generador que emitiera
    datos ya tipados haría imposible el escenario que Silver debe resolver.
    """

    def test_todas_las_columnas_pasan_a_texto(self) -> None:
        aterrizaje = schemas.to_landing_schema(schemas.ORDERS)
        for campo in aterrizaje:
            assert pa.types.is_string(campo.type), f"{campo.name} no es string"

    def test_conserva_los_nombres_y_su_orden(self) -> None:
        """Si el orden cambiara, un fichero crudo dejaría de corresponderse
        con su contrato lógico columna a columna."""
        assert schemas.to_landing_schema(schemas.ORDERS).names == schemas.ORDERS.names

    def test_no_altera_el_esquema_original(self) -> None:
        """pyarrow.Schema es inmutable, pero la función no debe depender de eso."""
        antes = schemas.ORDERS.field("total_amount").type
        schemas.to_landing_schema(schemas.ORDERS)
        assert schemas.ORDERS.field("total_amount").type == antes


class TestMetadatosDeLinaje:
    def test_declara_las_cuatro_columnas_de_linaje(self) -> None:
        assert set(metadata.LINEAGE_COLUMNS) == {
            "_ingestion_timestamp",
            "_source_file",
            "_batch_id",
            "_pipeline_run_id",
        }

    def test_los_metadatos_van_prefijados_con_guion_bajo(self) -> None:
        """El prefijo los distingue a simple vista de las columnas de negocio."""
        for columna in metadata.LINEAGE_COLUMNS:
            assert columna.startswith("_")

    @pytest.mark.parametrize("entidad", ["customers", "products", "orders", "order_items"])
    def test_ninguna_entidad_colisiona_con_los_metadatos(self, entidad: str) -> None:
        """Una colisión sobrescribiría en silencio un dato de negocio con
        metadatos de ingesta."""
        columnas = set(schemas.ALL_SCHEMAS[entidad].names)
        assert not columnas & set(metadata.LINEAGE_COLUMNS)
