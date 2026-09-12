"""Configuración compartida: perfiles de escala y nombres del namespace."""

from __future__ import annotations

import pytest

from ecommerce import config


class TestPerfilesDeEscala:
    def test_los_tres_perfiles_del_spec_existen(self) -> None:
        assert set(config.SCALE_PROFILES) == {"dev", "demo", "full"}

    def test_dev_tiene_el_tamano_del_spec(self) -> None:
        perfil = config.get_profile("dev")
        assert perfil.customers == 1_000
        assert perfil.products == 200
        assert perfil.orders == 10_000

    def test_los_perfiles_crecen_de_forma_monotona(self) -> None:
        """dev < demo < full en todas las dimensiones.

        Si alguien edita un perfil y rompe el orden, el perfil `dev` podría
        dejar de ser barato y agotar la cuota de cómputo de Free Edition.
        """
        dev, demo, full = (config.get_profile(n) for n in ("dev", "demo", "full"))
        for dimension in ("customers", "products", "orders", "web_events"):
            valores = [getattr(p, dimension) for p in (dev, demo, full)]
            assert valores == sorted(valores), f"{dimension} no crece: {valores}"
            assert len(set(valores)) == 3, f"{dimension} repite valores: {valores}"

    def test_perfil_desconocido_menciona_los_disponibles(self) -> None:
        """El error debe orientar, no solo fallar."""
        with pytest.raises(ValueError, match="dev"):
            config.get_profile("gigante")


class TestNamespace:
    def test_construye_nombres_de_tabla_cualificados(self) -> None:
        ns = config.Namespace()
        assert ns.table("bronze", "orders") == "ecommerce.bronze.orders"

    def test_el_catalogo_no_esta_codificado_en_el_codigo(self) -> None:
        """Reapuntar a otro catálogo debe ser un cambio de configuración.

        Free Edition podría obligar a trabajar bajo el catálogo `workspace`;
        el spec exige que eso no requiera tocar código.
        """
        ns = config.Namespace(catalog="workspace")
        assert ns.table("silver", "customers") == "workspace.silver.customers"
        assert ns.volume_root().startswith("/Volumes/workspace/")

    def test_la_raiz_del_volumen_apunta_a_la_zona_de_aterrizaje(self) -> None:
        assert config.Namespace().volume_root() == "/Volumes/ecommerce/landing/raw"

    def test_los_checkpoints_viven_fuera_de_la_zona_de_aterrizaje(self) -> None:
        """Auto Loader vigila `landing/raw/<entidad>/`. Escribir su propio
        estado ahí dentro haría que se detectara a sí mismo como datos nuevos."""
        ns = config.Namespace()
        assert not ns.checkpoints_root().startswith(ns.volume_root())
        assert ns.checkpoints_root() == "/Volumes/ecommerce/ops/checkpoints"


class TestRutasDeLote:
    def test_la_etiqueta_de_lote_se_rellena_con_ceros(self) -> None:
        """Con ceros a la izquierda, el orden lexicográfico coincide con el
        numérico: batch_002 antes que batch_010 al listar el directorio."""
        assert config.batch_label(0) == "batch_000"
        assert config.batch_label(31) == "batch_031"

    def test_la_ruta_de_lote_separa_por_entidad(self) -> None:
        """Auto Loader se suscribe a un directorio por entidad."""
        ruta = config.batch_path("/Volumes/ecommerce/landing/raw", "orders", 7)
        assert ruta == "/Volumes/ecommerce/landing/raw/orders/batch_007"


class TestVentanaTemporal:
    def test_la_fecha_de_referencia_es_fija_y_no_la_de_hoy(self) -> None:
        """Usar `date.today()` haría que el dataset cambiara cada día y
        destruiría la reproducibilidad, que es el punto del generador."""
        assert config.REFERENCE_DATE.year >= 2026
        assert config.HISTORY_MONTHS >= 12
