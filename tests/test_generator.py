"""El generador de datos.

Sin Spark: son segundos de ejecución, así que estos tests corren en cada push.

Lo que se verifica es lo que el proyecto afirma en su README —determinismo,
integridad referencial, suciedad explicable— porque una afirmación que nadie
comprueba es una afirmación que acaba siendo falsa.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ecommerce import schemas
from ecommerce.config import ScaleProfile
from ecommerce.generator import generate_batch
from ecommerce.generator.dirt import DirtRates

# Perfil diminuto: los tests deben ser instantáneos. Los tamaños son suficientes
# para que las tasas de suciedad den recuentos distintos de cero.
PERFIL_TEST = ScaleProfile("test", customers=500, products=100, orders=2_000, web_events=5_000)

# Tasas exageradas frente a las de producción, para que cada anomalía produzca
# un número de filas comprobable en un perfil pequeño.
TASAS_TEST = DirtRates(
    duplicate_orders=0.01,
    orphan_customer_id=0.01,
    non_positive_quantity=0.01,
    negative_price=0.01,
    invalid_timestamp=0.01,
    inconsistent_status=0.05,
    malformed_email=0.02,
)


def leer(root: Path, entidad: str, lote: int = 0):
    """Lee todas las partes de un lote de una entidad."""
    from ecommerce.config import batch_label

    return pq.read_table(root / entidad / batch_label(lote))


def columna(tabla, nombre: str) -> list:
    return tabla.column(nombre).to_pylist()


def primera_fecha_valida(ordenes) -> str:
    """Fecha de orden más antigua, descartando las inválidas inyectadas.

    Sin filtrar, `min()` sobre texto devolvería la cadena vacía que la
    inyección de suciedad coloca a propósito.
    """
    validas = []
    for valor in columna(ordenes, "order_date"):
        try:
            datetime.strptime(valor, "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            continue
        validas.append(valor)
    return min(validas)


@pytest.fixture
def lote(tmp_path: Path):
    """Un lote generado con semilla fija, compartido por varios tests."""
    manifiesto = generate_batch(
        profile=PERFIL_TEST, batch=0, seed=42, output_root=tmp_path, dirt_rates=TASAS_TEST
    )
    return tmp_path, manifiesto


class TestDeterminismo:
    def test_la_misma_semilla_produce_los_mismos_datos(self, tmp_path: Path) -> None:
        """Es la propiedad que hace reproducible el reporte de calidad.

        Sin ella, cada ejecución daría cifras distintas y ninguna afirmación
        del README sería verificable por un tercero.
        """
        a, b = tmp_path / "a", tmp_path / "b"
        for destino in (a, b):
            generate_batch(
                profile=PERFIL_TEST, batch=0, seed=42, output_root=destino, dirt_rates=TASAS_TEST
            )

        for entidad in schemas.ALL_SCHEMAS:
            assert leer(a, entidad).equals(leer(b, entidad)), f"{entidad} difiere entre ejecuciones"

    def test_semillas_distintas_producen_datos_distintos(self, tmp_path: Path) -> None:
        """Contrapartida del test anterior: descartaría una implementación que
        ignorase la semilla y devolviese siempre lo mismo."""
        a, b = tmp_path / "a", tmp_path / "b"
        generate_batch(profile=PERFIL_TEST, batch=0, seed=1, output_root=a, dirt_rates=TASAS_TEST)
        generate_batch(profile=PERFIL_TEST, batch=0, seed=2, output_root=b, dirt_rates=TASAS_TEST)
        assert not leer(a, "orders").equals(leer(b, "orders"))


class TestFormatoDeAterrizaje:
    def test_todas_las_columnas_llegan_como_texto(self, lote) -> None:
        """Requisito para poder inyectar valores inválidos."""
        root, _ = lote
        for entidad, esquema in schemas.ALL_SCHEMAS.items():
            tabla = leer(root, entidad)
            assert tabla.schema.names == esquema.names
            assert tabla.schema.equals(schemas.to_landing_schema(esquema))

    def test_los_ficheros_se_agrupan_por_entidad_y_lote(self, lote) -> None:
        root, _ = lote
        assert list((root / "orders" / "batch_000").glob("*.parquet"))

    def test_el_manifiesto_queda_fuera_de_los_directorios_de_entidad(self, lote) -> None:
        """Auto Loader se suscribe a `<entidad>/` y espera un esquema homogéneo.

        Un JSON dentro de ese árbol rompería la inferencia de esquema.
        """
        root, _ = lote
        assert not list((root / "orders").rglob("*.json"))
        assert (root / "_manifests" / "batch_000.json").exists()


class TestVolumen:
    def test_las_dimensiones_tienen_el_tamano_del_perfil(self, lote) -> None:
        root, _ = lote
        assert leer(root, "customers").num_rows == PERFIL_TEST.customers
        assert leer(root, "products").num_rows == PERFIL_TEST.products

    def test_las_ordenes_suman_el_perfil_mas_los_duplicados_inyectados(self, lote) -> None:
        """El recuento crudo NO coincide con el perfil, y debe ser explicable:
        la diferencia son exactamente los duplicados que se inyectaron."""
        root, manifiesto = lote
        duplicados = manifiesto["injected"]["orders"]["duplicate_orders"]
        assert leer(root, "orders").num_rows == PERFIL_TEST.orders + duplicados

    def test_cada_orden_tiene_al_menos_una_linea(self, lote) -> None:
        root, _ = lote
        assert leer(root, "order_items").num_rows >= PERFIL_TEST.orders


class TestVigenciaDeLasDimensiones:
    """La primera versión de una dimensión debe cubrir todo el histórico.

    Es la condición que hace posible unir un hecho con la versión de la
    dimensión vigente en su fecha. Si el snapshot inicial se marca con la fecha
    de **extracción** en lugar de la de creación del registro, ninguna orden
    anterior a esa extracción encuentra versión, y el coste y la categoría
    salen nulos en Gold para casi todo el histórico.
    """

    def test_los_clientes_existen_antes_de_la_primera_orden(self, lote) -> None:
        root, _ = lote
        primera_orden = primera_fecha_valida(leer(root, "orders"))
        for actualizado in columna(leer(root, "customers"), "updated_at"):
            assert actualizado <= primera_orden

    def test_los_productos_existen_antes_de_la_primera_orden(self, lote) -> None:
        root, _ = lote
        primera_orden = primera_fecha_valida(leer(root, "orders"))
        for actualizado in columna(leer(root, "products"), "updated_at"):
            assert actualizado <= primera_orden

    def test_el_cliente_se_marca_con_su_fecha_de_alta(self, lote) -> None:
        """La fecha de alta es cuando el registro pasó a existir, que es lo que
        `valid_from` debe reflejar en la primera versión."""
        root, _ = lote
        clientes = leer(root, "customers")
        altas = columna(clientes, "registration_date")
        actualizados = columna(clientes, "updated_at")
        for alta, actualizado in zip(altas, actualizados, strict=True):
            assert actualizado.startswith(alta)


class TestIntegridadReferencial:
    def test_los_huerfanos_son_exactamente_los_inyectados(self, lote) -> None:
        """Ninguna referencia rota es accidental: todas están contabilizadas."""
        root, manifiesto = lote
        clientes = set(columna(leer(root, "customers"), "customer_id"))
        referencias = columna(leer(root, "orders"), "customer_id")

        huerfanos = [c for c in referencias if c not in clientes]
        esperados = manifiesto["injected"]["orders"]["orphan_customer_id"]
        assert len(huerfanos) == esperados

    def test_toda_linea_apunta_a_una_orden_existente(self, lote) -> None:
        root, _ = lote
        ordenes = set(columna(leer(root, "orders"), "order_id"))
        assert set(columna(leer(root, "order_items"), "order_id")) <= ordenes

    def test_toda_linea_apunta_a_un_producto_existente(self, lote) -> None:
        root, _ = lote
        productos = set(columna(leer(root, "products"), "product_id"))
        assert set(columna(leer(root, "order_items"), "product_id")) <= productos


class TestInvariantesDeNegocio:
    """Las invariantes deben cumplirse en las filas limpias.

    Si no cuadran, cualquier KPI de Gold será incorrecto y el error aparecería
    muy tarde, cuando ya nadie mira el generador.
    """

    def test_el_total_de_linea_cuadra(self, lote) -> None:
        root, _ = lote
        items = leer(root, "order_items")
        cantidades = columna(items, "quantity")
        precios = columna(items, "unit_price")
        descuentos = columna(items, "discount_amount")
        totales = columna(items, "line_total")

        comprobadas = 0
        for cant, precio, desc, total in zip(cantidades, precios, descuentos, totales, strict=True):
            # Se saltan las filas ensuciadas: su incoherencia es intencionada.
            if int(cant) <= 0 or Decimal(precio) < 0:
                continue
            assert Decimal(total) == Decimal(cant) * Decimal(precio) - Decimal(desc)
            comprobadas += 1

        assert comprobadas > 0, "No se comprobó ninguna fila limpia"

    def test_el_neto_de_la_orden_es_el_bruto_menos_el_descuento(self, lote) -> None:
        root, _ = lote
        ordenes = leer(root, "orders")
        for bruto, desc, total in zip(
            columna(ordenes, "gross_amount"),
            columna(ordenes, "discount_amount"),
            columna(ordenes, "total_amount"),
            strict=True,
        ):
            assert Decimal(total) == Decimal(bruto) - Decimal(desc)

    def test_el_bruto_de_la_orden_es_la_suma_de_sus_lineas(self, lote) -> None:
        root, _ = lote
        items = leer(root, "order_items")

        sumas: dict[str, Decimal] = {}
        for oid, total, desc in zip(
            columna(items, "order_id"),
            columna(items, "line_total"),
            columna(items, "discount_amount"),
            strict=True,
        ):
            sumas[oid] = sumas.get(oid, Decimal(0)) + Decimal(total) + Decimal(desc)

        ordenes = leer(root, "orders")
        comprobadas = 0
        for oid, bruto in zip(
            columna(ordenes, "order_id"), columna(ordenes, "gross_amount"), strict=True
        ):
            if oid in sumas:
                assert Decimal(bruto) == sumas[oid], f"La orden {oid} no cuadra con sus líneas"
                comprobadas += 1
        assert comprobadas > 0


class TestSuciedadControlada:
    """Cada anomalía debe existir, ser contable y estar declarada."""

    def test_hay_ordenes_duplicadas(self, lote) -> None:
        root, manifiesto = lote
        ids = columna(leer(root, "orders"), "order_id")
        assert len(ids) - len(set(ids)) == manifiesto["injected"]["orders"]["duplicate_orders"]

    def test_hay_fechas_invalidas_no_parseables(self, lote) -> None:
        """Solo es posible porque la columna llega como texto.

        Se comprueba parseando de verdad, no mirando el prefijo: un valor como
        `2026-13-45` empieza por dígitos pero tampoco es una fecha válida, y
        Silver tiene que rechazarlo igual.
        """
        root, manifiesto = lote

        def parseable(valor: str | None) -> bool:
            if valor is None:
                return False
            try:
                datetime.strptime(valor, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return False
            return True

        fechas = columna(leer(root, "orders"), "order_date")
        invalidas = [f for f in fechas if not parseable(f)]
        assert len(invalidas) == manifiesto["injected"]["orders"]["invalid_timestamp"]

    def test_hay_estados_con_mayusculas_y_espacios_inconsistentes(self, lote) -> None:
        root, manifiesto = lote
        estados = columna(leer(root, "orders"), "status")
        sucios = [e for e in estados if e != e.strip() or e != e.lower()]
        assert len(sucios) == manifiesto["injected"]["orders"]["inconsistent_status"]

    def test_hay_cantidades_no_positivas(self, lote) -> None:
        root, manifiesto = lote
        cantidades = [int(c) for c in columna(leer(root, "order_items"), "quantity")]
        esperadas = manifiesto["injected"]["order_items"]["non_positive_quantity"]
        assert len([c for c in cantidades if c <= 0]) == esperadas

    def test_hay_precios_negativos(self, lote) -> None:
        root, manifiesto = lote
        precios = [Decimal(p) for p in columna(leer(root, "order_items"), "unit_price")]
        esperados = manifiesto["injected"]["order_items"]["negative_price"]
        assert len([p for p in precios if p < 0]) == esperados

    def test_hay_correos_malformados(self, lote) -> None:
        root, manifiesto = lote
        correos = columna(leer(root, "customers"), "email")
        malos = [c for c in correos if c is None or "@" not in c]
        assert len(malos) == manifiesto["injected"]["customers"]["malformed_email"]

    def test_una_tasa_de_cero_no_inyecta_nada(self, tmp_path: Path) -> None:
        """Permite generar un lote limpio para aislar fallos del pipeline."""
        manifiesto = generate_batch(
            profile=PERFIL_TEST,
            batch=0,
            seed=42,
            output_root=tmp_path,
            dirt_rates=DirtRates.limpio(),
        )
        for entidad in manifiesto["injected"].values():
            assert all(conteo == 0 for conteo in entidad.values())

        ids = columna(leer(tmp_path, "orders"), "order_id")
        assert len(ids) == len(set(ids)) == PERFIL_TEST.orders


class TestManifiesto:
    def test_registra_lo_necesario_para_reproducir_el_lote(self, lote) -> None:
        _, manifiesto = lote
        for clave in ("seed", "profile", "batch", "chunk_orders", "rates", "rows", "injected"):
            assert clave in manifiesto, f"Falta '{clave}' en el manifiesto"
        assert manifiesto["seed"] == 42
        assert manifiesto["profile"] == "test"

    def test_el_manifiesto_en_disco_coincide_con_el_devuelto(self, lote) -> None:
        root, manifiesto = lote
        en_disco = json.loads((root / "_manifests" / "batch_000.json").read_text(encoding="utf-8"))
        assert en_disco == manifiesto

    def test_los_recuentos_de_filas_coinciden_con_los_ficheros(self, lote) -> None:
        root, manifiesto = lote
        for entidad, filas in manifiesto["rows"].items():
            assert leer(root, entidad).num_rows == filas


class TestParticionadoEnPartes:
    def test_un_chunk_pequeno_produce_varias_partes(self, tmp_path: Path) -> None:
        """La generación por trozos acota la memoria: el perfil `full` son
        5 millones de órdenes y unos 12 millones de líneas, que no caben
        cómodamente en memoria de una vez."""
        generate_batch(
            profile=PERFIL_TEST,
            batch=0,
            seed=42,
            output_root=tmp_path,
            dirt_rates=TASAS_TEST,
            chunk_orders=500,
        )
        partes = list((tmp_path / "orders" / "batch_000").glob("*.parquet"))
        assert len(partes) == 4

    def test_el_troceado_no_cambia_el_total_de_filas(self, tmp_path: Path) -> None:
        a, b = tmp_path / "a", tmp_path / "b"
        for destino, chunk in ((a, 500), (b, 10_000)):
            generate_batch(
                profile=PERFIL_TEST,
                batch=0,
                seed=42,
                output_root=destino,
                dirt_rates=TASAS_TEST,
                chunk_orders=chunk,
            )
        assert leer(a, "orders").num_rows == leer(b, "orders").num_rows

    def test_los_identificadores_son_unicos_entre_partes(self, tmp_path: Path) -> None:
        """Un contador reiniciado por trozo generaría claves repetidas."""
        generate_batch(
            profile=PERFIL_TEST,
            batch=0,
            seed=42,
            output_root=tmp_path,
            dirt_rates=DirtRates.limpio(),
            chunk_orders=500,
        )
        ids = columna(leer(tmp_path, "order_items"), "order_item_id")
        assert len(ids) == len(set(ids))


class TestPagos:
    def test_todo_pago_apunta_a_una_orden_existente(self, lote) -> None:
        root, _ = lote
        ordenes = set(columna(leer(root, "orders"), "order_id"))
        assert set(columna(leer(root, "payments"), "order_id")) <= ordenes

    def test_el_importe_del_pago_coincide_con_el_de_la_orden(self, lote) -> None:
        """Un pago que no cuadra con su orden es una incoherencia entre sistemas
        de origen. Aquí no debe haberla: toda anomalía del dataset es
        intencionada y está declarada en el manifiesto."""
        root, _ = lote
        ordenes = leer(root, "orders")
        totales = dict(
            zip(columna(ordenes, "order_id"), columna(ordenes, "total_amount"), strict=True)
        )

        pagos = leer(root, "payments")
        for oid, importe in zip(columna(pagos, "order_id"), columna(pagos, "amount"), strict=True):
            assert Decimal(importe) == Decimal(totales[oid])

    def test_el_estado_del_pago_es_coherente_con_el_de_la_orden(self, lote) -> None:
        """Una orden entregada no puede tener el pago pendiente."""
        root, _ = lote
        ordenes = leer(root, "orders")
        estado_orden = dict(
            zip(columna(ordenes, "order_id"), columna(ordenes, "status"), strict=True)
        )

        pagos = leer(root, "payments")
        for oid, estado in zip(
            columna(pagos, "order_id"), columna(pagos, "payment_status"), strict=True
        ):
            if estado_orden[oid].strip().lower() == "delivered":
                assert estado == "completed"


class TestResenas:
    def test_solo_se_resena_lo_que_se_recibio(self, lote) -> None:
        """Nadie reseña un pedido cancelado o que aún no ha llegado."""
        root, _ = lote
        ordenes = leer(root, "orders")
        estado = dict(zip(columna(ordenes, "order_id"), columna(ordenes, "status"), strict=True))

        for oid in columna(leer(root, "reviews"), "order_id"):
            assert estado[oid].strip().lower() == "delivered"

    def test_la_puntuacion_esta_dentro_del_dominio(self, lote) -> None:
        root, _ = lote
        puntuaciones = [int(r) for r in columna(leer(root, "reviews"), "rating")]
        assert puntuaciones
        assert all(1 <= p <= 5 for p in puntuaciones)


class TestDevoluciones:
    def test_toda_devolucion_apunta_a_una_linea_existente(self, lote) -> None:
        root, _ = lote
        lineas = set(columna(leer(root, "order_items"), "order_item_id"))
        devoluciones = columna(leer(root, "returns"), "order_item_id")
        assert devoluciones
        assert set(devoluciones) <= lineas

    def test_no_se_devuelven_mas_unidades_de_las_compradas(self, lote) -> None:
        root, _ = lote
        items = leer(root, "order_items")
        compradas = dict(
            zip(columna(items, "order_item_id"), columna(items, "quantity"), strict=True)
        )

        devoluciones = leer(root, "returns")
        comprobadas = 0
        for iid, cantidad in zip(
            columna(devoluciones, "order_item_id"), columna(devoluciones, "quantity"), strict=True
        ):
            # Las líneas con cantidad ensuciada se saltan: las devoluciones se
            # derivan del dato limpio, así que compararlas contra una cantidad
            # corrupta a propósito no diría nada sobre el generador.
            if int(compradas[iid]) <= 0:
                continue
            assert int(cantidad) <= int(compradas[iid])
            comprobadas += 1

        assert comprobadas > 0

    def test_el_reembolso_no_supera_el_total_de_la_linea(self, lote) -> None:
        """Devolver más dinero del cobrado sería un error contable."""
        root, _ = lote
        items = leer(root, "order_items")
        totales = dict(
            zip(columna(items, "order_item_id"), columna(items, "line_total"), strict=True)
        )

        devoluciones = leer(root, "returns")
        for iid, reembolso in zip(
            columna(devoluciones, "order_item_id"),
            columna(devoluciones, "refund_amount"),
            strict=True,
        ):
            assert Decimal(reembolso) <= Decimal(totales[iid])


class TestEventosWeb:
    def test_genera_el_numero_de_eventos_del_perfil(self, lote) -> None:
        root, _ = lote
        assert leer(root, "web_events").num_rows == PERFIL_TEST.web_events

    def test_los_tipos_de_evento_son_los_del_embudo(self, lote) -> None:
        root, _ = lote
        tipos = set(columna(leer(root, "web_events"), "event_type"))
        assert tipos <= {"page_view", "add_to_cart", "checkout_start", "purchase"}

    def test_el_embudo_se_estrecha_en_cada_paso(self, lote) -> None:
        """Es la propiedad que hace que `marketing_funnel` tenga sentido.

        Si hubiera más compras que vistas de página, la tasa de conversión
        superaría el 100 % y el dashboard sería absurdo.
        """
        root, _ = lote
        tipos = columna(leer(root, "web_events"), "event_type")
        conteo = {t: tipos.count(t) for t in set(tipos)}

        assert conteo["page_view"] >= conteo.get("add_to_cart", 0)
        assert conteo.get("add_to_cart", 0) >= conteo.get("checkout_start", 0)
        assert conteo.get("checkout_start", 0) >= conteo.get("purchase", 0)

    def test_hay_trafico_anonimo(self, lote) -> None:
        """La mayor parte del tráfico de un e-commerce no ha iniciado sesión."""
        root, _ = lote
        clientes = columna(leer(root, "web_events"), "customer_id")
        assert any(c is None for c in clientes)
        assert any(c is not None for c in clientes)

    def test_los_eventos_de_una_sesion_comparten_identificador(self, lote) -> None:
        """Sin sesiones agrupables no se puede medir un embudo."""
        root, _ = lote
        sesiones = columna(leer(root, "web_events"), "session_id")
        assert len(set(sesiones)) < len(sesiones)


class TestCLI:
    def test_genera_un_lote_desde_la_linea_de_comandos(self, tmp_path: Path) -> None:
        from ecommerce.generator import cli

        codigo = cli.main(["--profile", "dev", "--seed", "7", "--out", str(tmp_path)])

        assert codigo == 0
        assert leer(tmp_path, "orders").num_rows > 0

    def test_la_bandera_limpio_desactiva_toda_la_suciedad(self, tmp_path: Path) -> None:
        """Un lote sin anomalías permite distinguir un fallo del pipeline de un
        dato malo, que de otro modo se confunden."""
        from ecommerce.generator import cli

        cli.main(["--profile", "dev", "--seed", "7", "--out", str(tmp_path), "--limpio"])

        manifiesto = json.loads(
            (tmp_path / "_manifests" / "batch_000.json").read_text(encoding="utf-8")
        )
        assert all(tasa == 0.0 for tasa in manifiesto["rates"].values())

    def test_un_perfil_inexistente_falla_sin_escribir_nada(self, tmp_path: Path) -> None:
        from ecommerce.generator import cli

        with pytest.raises(SystemExit):
            cli.main(["--profile", "gigante", "--out", str(tmp_path)])
        assert not (tmp_path / "orders").exists()
