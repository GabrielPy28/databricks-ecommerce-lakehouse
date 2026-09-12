"""Eventos de navegación.

Es la tabla más grande del proyecto —50 millones de filas en el perfil `full`—
y la que alimenta `gold.marketing_funnel`.

La estructura clave es el **embudo**: una sesión avanza por los pasos en orden
y se abandona en alguno. De ahí sale que cada paso tenga menos eventos que el
anterior, sin lo cual la tasa de conversión superaría el 100 % y el dashboard
sería absurdo.
"""

from __future__ import annotations

import numpy as np

from ecommerce.generator import serialize
from ecommerce.generator.dimensions import GeneratedEntity

# Pasos del embudo, en orden. Una sesión con profundidad `d` emite los `d`
# primeros.
EMBUDO = np.array(["page_view", "add_to_cart", "checkout_start", "purchase"])

# Reparto de profundidad: la mayoría de las visitas se queda en la portada.
PESO_PROFUNDIDAD = np.array([0.55, 0.24, 0.13, 0.08])

DISPOSITIVOS = np.array(["mobile", "desktop", "tablet"])
PESO_DISPOSITIVOS = np.array([0.62, 0.33, 0.05])

CANALES = np.array(["organic", "paid_search", "social", "email", "direct", "referral"])
PESO_CANALES = np.array([0.30, 0.22, 0.18, 0.12, 0.13, 0.05])

# Proporción de sesiones con cliente identificado. El resto es tráfico anónimo,
# que es la mayoría en cualquier e-commerce real.
PROB_IDENTIFICADO = 0.28

# Segundos entre pasos consecutivos de una misma sesión.
SEGUNDOS_ENTRE_PASOS = 45

# Sesiones de más que se generan antes de recortar, para que la variación
# aleatoria de la profundidad nunca deje el lote por debajo de lo pedido.
MARGEN_SESIONES = 1.05


def generate_web_events(
    *,
    n_events: int,
    customers: GeneratedEntity,
    products: GeneratedEntity,
    product_cdf: np.ndarray,
    rng: np.random.Generator,
    offset: int,
    inicio: np.datetime64,
    dias: int,
) -> dict[str, np.ndarray]:
    """Genera exactamente `n_events` eventos, agrupados en sesiones."""
    if n_events <= 0:
        return {nombre: np.empty(0, dtype=object).astype(str) for nombre in _COLUMNAS}

    # Se generan sesiones de más y luego se recorta al número pedido. El corte
    # deja la última sesión a medias, que es justo lo que ocurre con las
    # sesiones aún en curso cuando se extraen los datos.
    #
    # El margen no es opcional: la profundidad es aleatoria, así que estimar el
    # número de sesiones con la profundidad *media* se queda corto la mitad de
    # las veces. Sin él, pedir 5.000.000 de eventos devolvía 4.999.673, y el
    # fallo solo aparecía a escalas grandes.
    profundidad_media = float((np.arange(1, 5) * PESO_PROFUNDIDAD).sum())
    n_sesiones = int(n_events / profundidad_media * MARGEN_SESIONES) + 10

    profundidad = rng.choice(np.arange(1, 5), size=n_sesiones, p=PESO_PROFUNDIDAD)

    # Atributos de sesión, repetidos en cada evento suyo: un dispositivo no
    # cambia a mitad de visita.
    sesiones = serialize.ids("SES", offset, n_sesiones, 10)
    dispositivos = rng.choice(DISPOSITIVOS, size=n_sesiones, p=PESO_DISPOSITIVOS)
    canales = rng.choice(CANALES, size=n_sesiones, p=PESO_CANALES)

    identificado = rng.random(n_sesiones) < PROB_IDENTIFICADO
    idx_cliente = rng.integers(0, len(customers.columns["customer_id"]), n_sesiones)
    cliente_sesion = np.where(identificado, customers.columns["customer_id"][idx_cliente], None)

    # Un producto por sesión: el visitante mira, añade y compra lo mismo.
    idx_producto = serialize.weighted_index(product_cdf, rng, n_sesiones)
    producto_sesion = products.columns["product_id"][idx_producto]

    inicio_sesion = inicio + (rng.random(n_sesiones) * dias * 86_400).astype("timedelta64[s]")

    # Expansión a eventos.
    paso = _indices_dentro_de_grupo(profundidad)
    repetir = profundidad

    eventos = {
        "event_id": serialize.ids("EVT", offset, int(profundidad.sum()), 12),
        "event_timestamp": serialize.timestamps_to_str(
            np.repeat(inicio_sesion, repetir)
            + (paso * SEGUNDOS_ENTRE_PASOS).astype("timedelta64[s]")
        ),
        "session_id": np.repeat(sesiones, repetir),
        "customer_id": np.repeat(cliente_sesion, repetir),
        "event_type": EMBUDO[paso],
        # La portada no mira ningún producto: el primer evento va sin él.
        "product_id": np.where(paso == 0, None, np.repeat(producto_sesion, repetir)),
        "device": np.repeat(dispositivos, repetir),
        "utm_source": np.repeat(canales, repetir),
    }

    generados = len(eventos["event_id"])
    if generados < n_events:  # pragma: no cover - el margen lo hace inalcanzable
        raise RuntimeError(
            f"Se generaron {generados} eventos y se pedían {n_events}. Sube MARGEN_SESIONES."
        )

    return {nombre: valores[:n_events] for nombre, valores in eventos.items()}


_COLUMNAS = (
    "event_id",
    "event_timestamp",
    "session_id",
    "customer_id",
    "event_type",
    "product_id",
    "device",
    "utm_source",
)


def _indices_dentro_de_grupo(tamanos: np.ndarray) -> np.ndarray:
    """Para `[2, 3]` devuelve `[0, 1, 0, 1, 2]`.

    Es la posición de cada evento dentro de su sesión, y por tanto el paso del
    embudo que representa. Vectorizado: un bucle sobre 50 millones de eventos
    tardaría minutos.
    """
    total = int(tamanos.sum())
    cortes = np.concatenate([[0], np.cumsum(tamanos)[:-1]])
    return np.arange(total) - np.repeat(cortes, tamanos)
