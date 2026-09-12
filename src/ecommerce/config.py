"""Configuración compartida por generador, ingesta y transformaciones.

Un único sitio donde viven los nombres del namespace y los tamaños de los
perfiles. El objetivo es que reapuntar el proyecto a otro catálogo, o cambiar
la escala de una ejecución, no requiera tocar el código.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date

# --------------------------------------------------------------------------
# Ventana temporal
# --------------------------------------------------------------------------

# Fecha fija a propósito. Con `date.today()` el dataset cambiaría cada día y
# se perdería la reproducibilidad, que es la razón de ser del generador: dos
# ejecuciones con la misma semilla deben producir exactamente lo mismo.
REFERENCE_DATE = date(2026, 9, 1)

# Profundidad del histórico que cubre el lote inicial.
HISTORY_MONTHS = 24


# --------------------------------------------------------------------------
# Perfiles de escala
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ScaleProfile:
    """Tamaño de una ejecución del generador.

    `orders` cuenta cabeceras de pedido; las líneas de detalle se derivan de
    ellas, así que `order_items` no se configura: sale de la distribución de
    artículos por pedido.
    """

    name: str
    customers: int
    products: int
    orders: int
    web_events: int


SCALE_PROFILES: dict[str, ScaleProfile] = {
    # Iteración y tests. Debe seguir siendo barato: es el perfil que se ejecuta
    # decenas de veces al día.
    "dev": ScaleProfile("dev", customers=1_000, products=200, orders=10_000, web_events=100_000),
    # Ejecuciones normales de demostración.
    "demo": ScaleProfile(
        "demo", customers=50_000, products=2_000, orders=500_000, web_events=5_000_000
    ),
    # Una sola corrida, al final, para las métricas reales del README.
    "full": ScaleProfile(
        "full", customers=200_000, products=5_000, orders=5_000_000, web_events=50_000_000
    ),
}


def get_profile(name: str) -> ScaleProfile:
    """Devuelve un perfil por nombre.

    Ante un nombre desconocido enumera los disponibles: un `KeyError` pelado
    obligaría a ir a buscar el fichero.
    """
    try:
        return SCALE_PROFILES[name]
    except KeyError:
        disponibles = ", ".join(SCALE_PROFILES)
        raise ValueError(f"Perfil desconocido '{name}'. Disponibles: {disponibles}") from None


# --------------------------------------------------------------------------
# Namespace en Unity Catalog
# --------------------------------------------------------------------------

# Free Edition podría obligar a trabajar bajo el catálogo `workspace`. Leerlo
# del entorno hace que ese cambio no toque una sola línea de código.
DEFAULT_CATALOG = os.environ.get("ECOMMERCE_CATALOG", "ecommerce")


@dataclass(frozen=True)
class Namespace:
    """Nombres cualificados del namespace del proyecto."""

    catalog: str = DEFAULT_CATALOG
    landing_schema: str = "landing"
    landing_volume: str = "raw"
    ops_schema: str = "ops"

    def table(self, layer: str, name: str) -> str:
        """Nombre de tres niveles: `catalogo.capa.tabla`."""
        return f"{self.catalog}.{layer}.{name}"

    def volume_root(self) -> str:
        """Ruta del Volume donde aterrizan los ficheros crudos."""
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.landing_volume}"

    def checkpoints_root(self) -> str:
        """Ruta del Volume donde Auto Loader guarda esquemas y checkpoints.

        Deliberadamente fuera de la zona de aterrizaje: Auto Loader vigila
        `landing/raw/<entidad>/` en busca de ficheros nuevos, y si escribiera
        su propio estado ahí dentro acabaría detectándose a sí mismo.
        """
        return f"/Volumes/{self.catalog}/{self.ops_schema}/checkpoints"


# --------------------------------------------------------------------------
# Rutas de lote
# --------------------------------------------------------------------------

# Ancho del contador de lote. Con ceros a la izquierda el orden lexicográfico
# coincide con el numérico, así que listar el directorio ya sale ordenado.
BATCH_LABEL_WIDTH = 3


def batch_label(numero: int) -> str:
    """`0` -> `batch_000`."""
    return f"batch_{numero:0{BATCH_LABEL_WIDTH}d}"


def batch_path(root: str, entity: str, numero: int) -> str:
    """Ruta de un lote de una entidad: `<root>/<entidad>/batch_NNN`.

    Un directorio por entidad porque Auto Loader se suscribe a un directorio y
    espera un esquema homogéneo dentro de él.
    """
    return f"{root.rstrip('/')}/{entity}/{batch_label(numero)}"
