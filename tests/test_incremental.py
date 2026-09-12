"""Lotes incrementales: mutaciones, datos tardíos y cambios de dimensión.

El lote 0 es la carga inicial. Los siguientes traen tres cosas que un `append`
ingenuo no sabe manejar:

* **Órdenes nuevas**, que solo hay que insertar.
* **Órdenes mutadas**: la misma clave con un estado posterior. Hay que
  actualizar, no duplicar.
* **Datos tardíos**: registros que pertenecen a una ventana anterior pero
  llegan ahora. No deben revertir el estado actual.

Y además cambios en las dimensiones, que alimentan el historial SCD2.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ecommerce.config import ScaleProfile, batch_label
from ecommerce.generator import generate_batch
from ecommerce.generator.dirt import DirtRates

PERFIL = ScaleProfile("test", customers=300, products=60, orders=1_500, web_events=2_000)


def leer(root: Path, entidad: str, lote: int):
    return pq.read_table(root / entidad / batch_label(lote))


def columna(tabla, nombre: str) -> list:
    return tabla.column(nombre).to_pylist()


def como_dict(tabla, clave: str, valor: str) -> dict:
    return dict(zip(columna(tabla, clave), columna(tabla, valor), strict=True))


@pytest.fixture
def dos_lotes(tmp_path: Path):
    """Lote inicial más un lote incremental, con la misma semilla."""
    manifiestos = [
        generate_batch(
            profile=PERFIL,
            batch=n,
            seed=7,
            output_root=tmp_path,
            dirt_rates=DirtRates.limpio(),
        )
        for n in (0, 1)
    ]
    return tmp_path, manifiestos


class TestOrdenesNuevas:
    def test_el_lote_incremental_trae_ordenes_que_no_existian(self, dos_lotes) -> None:
        root, _ = dos_lotes
        iniciales = set(columna(leer(root, "orders", 0), "order_id"))
        nuevas = set(columna(leer(root, "orders", 1), "order_id"))
        assert nuevas - iniciales

    def test_los_identificadores_continuan_la_secuencia(self, dos_lotes) -> None:
        """Reiniciar el contador haría que una orden nueva reutilizara la clave
        de una antigua, y el MERGE la sobrescribiría en silencio."""
        root, _ = dos_lotes
        iniciales = set(columna(leer(root, "orders", 0), "order_id"))
        nuevas = [o for o in columna(leer(root, "orders", 1), "order_id") if o not in iniciales]
        assert min(nuevas) > max(iniciales)

    def test_el_lote_incremental_es_mucho_menor_que_la_carga_inicial(self, dos_lotes) -> None:
        root, _ = dos_lotes
        assert leer(root, "orders", 1).num_rows < leer(root, "orders", 0).num_rows // 5


class TestMutaciones:
    def test_reaparecen_ordenes_del_lote_anterior(self, dos_lotes) -> None:
        root, _ = dos_lotes
        iniciales = set(columna(leer(root, "orders", 0), "order_id"))
        repetidas = [o for o in columna(leer(root, "orders", 1), "order_id") if o in iniciales]
        assert repetidas, "Sin mutaciones, el MERGE nunca actualizaría nada"

    def test_la_orden_mutada_trae_una_marca_de_actualizacion_posterior(self, dos_lotes) -> None:
        """Es lo que permite al MERGE decidir qué versión es la buena."""
        root, _ = dos_lotes
        antes = como_dict(leer(root, "orders", 0), "order_id", "updated_at")
        despues = leer(root, "orders", 1)

        comprobadas = 0
        for oid, actualizado in zip(
            columna(despues, "order_id"), columna(despues, "updated_at"), strict=True
        ):
            if oid in antes:
                assert actualizado > antes[oid]
                comprobadas += 1
        assert comprobadas > 0

    def test_la_mutacion_cambia_el_estado(self, dos_lotes) -> None:
        root, _ = dos_lotes
        antes = como_dict(leer(root, "orders", 0), "order_id", "status")
        despues = leer(root, "orders", 1)

        cambios = [
            (oid, estado)
            for oid, estado in zip(
                columna(despues, "order_id"), columna(despues, "status"), strict=True
            )
            if oid in antes and estado != antes[oid]
        ]
        assert cambios

    def test_la_mutacion_conserva_la_identidad_de_la_orden(self, dos_lotes) -> None:
        """Una orden que avanza de estado sigue siendo del mismo cliente y por
        el mismo importe. Cambiarlos la convertiría en otra orden distinta."""
        root, _ = dos_lotes
        antes_cliente = como_dict(leer(root, "orders", 0), "order_id", "customer_id")
        antes_total = como_dict(leer(root, "orders", 0), "order_id", "total_amount")
        despues = leer(root, "orders", 1)

        for oid, cliente, total in zip(
            columna(despues, "order_id"),
            columna(despues, "customer_id"),
            columna(despues, "total_amount"),
            strict=True,
        ):
            if oid in antes_cliente:
                assert cliente == antes_cliente[oid]
                assert total == antes_total[oid]

    def test_los_estados_avanzan_y_no_retroceden(self, dos_lotes) -> None:
        """Una orden entregada no vuelve a pendiente."""
        from ecommerce.generator.mutations import ORDEN_DEL_CICLO

        root, _ = dos_lotes
        antes = como_dict(leer(root, "orders", 0), "order_id", "status")
        despues = leer(root, "orders", 1)

        for oid, estado in zip(
            columna(despues, "order_id"), columna(despues, "status"), strict=True
        ):
            if oid in antes:
                assert ORDEN_DEL_CICLO[estado] >= ORDEN_DEL_CICLO[antes[oid]]


class TestDatosTardios:
    def test_algunas_ordenes_pertenecen_a_la_ventana_anterior(self, dos_lotes) -> None:
        """El caso que rompe los pipelines ingenuos: el registro llega ahora
        pero ocurrió antes."""
        root, manifiestos = dos_lotes
        inicio_ventana = datetime.fromisoformat(manifiestos[1]["window"]["start"])

        iniciales = set(columna(leer(root, "orders", 0), "order_id"))
        lote = leer(root, "orders", 1)

        tardias = [
            fecha
            for oid, fecha in zip(
                columna(lote, "order_id"), columna(lote, "order_date"), strict=True
            )
            if oid not in iniciales and datetime.fromisoformat(fecha) < inicio_ventana
        ]
        assert tardias

    def test_el_manifiesto_declara_cuantos_tardios_hay(self, dos_lotes) -> None:
        _, manifiestos = dos_lotes
        assert manifiestos[1]["incremental"]["late_orders"] > 0


class TestCambiosDeDimension:
    def test_algunos_clientes_cambian_de_atributo(self, dos_lotes) -> None:
        """Alimenta el historial SCD2: sin cambios, no hay nada que historificar."""
        root, _ = dos_lotes
        antes = como_dict(leer(root, "customers", 0), "customer_id", "country")
        despues = leer(root, "customers", 1)

        cambios = [
            cid
            for cid, pais in zip(
                columna(despues, "customer_id"), columna(despues, "country"), strict=True
            )
            if antes.get(cid) not in (None, pais)
        ]
        assert cambios

    def test_el_cliente_cambiado_trae_marca_posterior(self, dos_lotes) -> None:
        root, _ = dos_lotes
        antes = como_dict(leer(root, "customers", 0), "customer_id", "updated_at")
        despues = leer(root, "customers", 1)
        for cid, actualizado in zip(
            columna(despues, "customer_id"), columna(despues, "updated_at"), strict=True
        ):
            assert actualizado > antes[cid]

    def test_solo_se_reenvian_los_clientes_que_cambiaron(self, dos_lotes) -> None:
        """Reenviar la dimensión entera en cada lote funcionaría, pero manda a
        Bronze cientos de miles de filas idénticas por un puñado de cambios."""
        root, _ = dos_lotes
        assert leer(root, "customers", 1).num_rows < leer(root, "customers", 0).num_rows

    def test_los_productos_tambien_cambian_de_precio(self, dos_lotes) -> None:
        root, _ = dos_lotes
        antes = como_dict(leer(root, "products", 0), "product_id", "price")
        despues = leer(root, "products", 1)
        cambios = [
            pid
            for pid, precio in zip(
                columna(despues, "product_id"), columna(despues, "price"), strict=True
            )
            if antes[pid] != precio
        ]
        assert cambios


class TestDeterminismo:
    def test_el_mismo_lote_incremental_se_reproduce(self, tmp_path: Path) -> None:
        a, b = tmp_path / "a", tmp_path / "b"
        for destino in (a, b):
            for n in (0, 1):
                generate_batch(
                    profile=PERFIL,
                    batch=n,
                    seed=7,
                    output_root=destino,
                    dirt_rates=DirtRates.limpio(),
                )
        assert leer(a, "orders", 1).equals(leer(b, "orders", 1))


class TestManifiestoIncremental:
    def test_declara_la_composicion_del_lote(self, dos_lotes) -> None:
        """Sin este desglose, el número de filas de un lote incremental no se
        puede explicar: no se sabe cuántas son nuevas y cuántas mutaciones."""
        _, manifiestos = dos_lotes
        incremental = manifiestos[1]["incremental"]
        for clave in ("new_orders", "mutated_orders", "late_orders", "changed_customers"):
            assert clave in incremental

    def test_la_ventana_del_lote_avanza_en_el_tiempo(self, dos_lotes) -> None:
        _, manifiestos = dos_lotes
        fin_inicial = datetime.fromisoformat(manifiestos[0]["window"]["end"])
        inicio_incremental = datetime.fromisoformat(manifiestos[1]["window"]["start"])
        assert inicio_incremental >= fin_inicial
