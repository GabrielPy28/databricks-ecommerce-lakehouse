"""Entidades que derivan de las órdenes: pagos, reseñas y devoluciones.

Las tres se construyen desde `ChunkInternals`, es decir, desde los datos
**limpios y sin serializar** del trozo de órdenes. Dos motivos:

* Operar sobre texto para sumar días a una fecha o comparar importes sería
  frágil y absurdo.
* En la realidad son sistemas de origen distintos. Que un extracto de órdenes
  venga sucio no implica que el de pagos lo esté, así que la suciedad no se
  propaga entre ellos.
"""

from __future__ import annotations

import numpy as np

from ecommerce.generator import serialize
from ecommerce.generator.facts import ChunkInternals

# Estado del pago según el de la orden. Una orden entregada con el pago
# pendiente sería una incoherencia entre sistemas, y en este dataset toda
# anomalía es intencionada y está declarada.
ESTADO_PAGO = {
    "delivered": "completed",
    "shipped": "completed",
    "paid": "completed",
    "pending": "pending",
    "cancelled": "failed",
    "returned": "refunded",
}

# Proporción de órdenes con pago registrado. Las que faltan son el caso real
# del pago que nunca llegó a iniciarse.
PROB_PAGO = 0.98

# Proporción de órdenes entregadas que reciben reseña.
PROB_RESENA = 0.30

# Puntuaciones sesgadas al alza, como en cualquier catálogo real.
PUNTUACIONES = np.array([1, 2, 3, 4, 5])
PESO_PUNTUACIONES = np.array([0.04, 0.06, 0.12, 0.30, 0.48])

# Días entre la entrega y la reseña.
DIAS_RESENA_MIN, DIAS_RESENA_MAX = 3, 30

# Proporción de líneas de órdenes entregadas que se devuelven.
PROB_DEVOLUCION = 0.04
DIAS_DEVOLUCION_MIN, DIAS_DEVOLUCION_MAX = 5, 30

MOTIVOS_DEVOLUCION = np.array(
    ["damaged", "wrong_item", "not_as_described", "changed_mind", "size_issue", "late_delivery"]
)
PESO_MOTIVOS = np.array([0.18, 0.12, 0.20, 0.28, 0.15, 0.07])


def _dias(rng: np.random.Generator, minimo: int, maximo: int, cuantos: int) -> np.ndarray:
    return rng.integers(minimo, maximo, cuantos).astype("timedelta64[D]")


def generate_payments(
    internos: ChunkInternals, rng: np.random.Generator, offset: int
) -> dict[str, np.ndarray]:
    """Un pago por orden, con el importe y el estado de esa orden."""
    con_pago = rng.random(len(internos.ids_orden)) < PROB_PAGO
    posiciones = np.flatnonzero(con_pago)
    n = len(posiciones)

    estados_orden = internos.estados[posiciones]
    estados_pago = np.array([ESTADO_PAGO[e] for e in estados_orden], dtype=object)

    # El pago ocurre en los minutos siguientes a la orden.
    fechas = internos.fechas[posiciones] + rng.integers(10, 900, n).astype("timedelta64[s]")

    return {
        "payment_id": serialize.ids("PAY", offset, n, 9),
        "order_id": internos.ids_orden[posiciones],
        "payment_method": internos.metodos_pago[posiciones],
        "amount": serialize.money_to_str(internos.total_cents[posiciones]),
        "payment_status": estados_pago.astype(str),
        "payment_date": serialize.timestamps_to_str(fechas),
        "updated_at": serialize.timestamps_to_str(fechas),
    }


def generate_reviews(
    internos: ChunkInternals, rng: np.random.Generator, offset: int
) -> dict[str, np.ndarray]:
    """Reseñas de órdenes entregadas.

    Solo se reseña lo que se recibió: nadie puntúa un pedido cancelado o que
    aún no ha llegado, y permitirlo falsearía la puntuación media por producto.
    """
    entregadas = np.flatnonzero(
        (internos.estados == "delivered") & (rng.random(len(internos.estados)) < PROB_RESENA)
    )
    n = len(entregadas)

    # El producto reseñado es uno de los que llevaba la orden, elegido entre sus
    # líneas: reseñar un producto que no se compró no tendría sentido.
    desplazamiento = (rng.random(n) * internos.por_orden[entregadas]).astype(np.int64)
    idx_linea = internos.cortes[entregadas] + desplazamiento

    fechas = internos.fechas[entregadas] + _dias(rng, DIAS_RESENA_MIN, DIAS_RESENA_MAX, n)

    return {
        "review_id": serialize.ids("REV", offset, n, 9),
        "order_id": internos.ids_orden[entregadas],
        "product_id": internos.ids_producto_linea[idx_linea],
        "customer_id": internos.ids_cliente[entregadas],
        "rating": serialize.ints_to_str(rng.choice(PUNTUACIONES, size=n, p=PESO_PUNTUACIONES)),
        "review_date": serialize.timestamps_to_str(fechas),
    }


def generate_returns(
    internos: ChunkInternals, rng: np.random.Generator, offset: int
) -> dict[str, np.ndarray]:
    """Devoluciones a nivel de línea.

    El grano es la línea y no el pedido: se devuelve un artículo concreto, y es
    lo que necesita `product_performance.return_rate`.
    """
    # Solo las líneas de órdenes entregadas son devolubles.
    entregada_por_linea = np.repeat(internos.estados == "delivered", internos.por_orden)
    candidatas = np.flatnonzero(
        entregada_por_linea & (rng.random(len(internos.ids_linea)) < PROB_DEVOLUCION)
    )
    n = len(candidatas)

    unidades_compradas = internos.unidades[candidatas]
    # Nunca más unidades de las compradas: devolver de más sería un error
    # contable, no una anomalía de datos.
    unidades_devueltas = (rng.random(n) * unidades_compradas).astype(np.int64) + 1

    # El reembolso es proporcional a las unidades devueltas, así que jamás
    # supera el total de la línea.
    reembolso = internos.total_linea_cents[candidatas] * unidades_devueltas // unidades_compradas

    orden_de_linea = np.repeat(internos.ids_orden, internos.por_orden)
    fecha_de_linea = np.repeat(internos.fechas, internos.por_orden)
    fechas = fecha_de_linea[candidatas] + _dias(rng, DIAS_DEVOLUCION_MIN, DIAS_DEVOLUCION_MAX, n)

    return {
        "return_id": serialize.ids("RET", offset, n, 9),
        "order_id": orden_de_linea[candidatas],
        "order_item_id": internos.ids_linea[candidatas],
        "product_id": internos.ids_producto_linea[candidatas],
        "quantity": serialize.ints_to_str(unidades_devueltas),
        "reason": rng.choice(MOTIVOS_DEVOLUCION, size=n, p=PESO_MOTIVOS),
        "refund_amount": serialize.money_to_str(reembolso),
        "return_date": serialize.timestamps_to_str(fechas),
    }
