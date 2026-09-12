"""Generación de los hechos: órdenes y sus líneas.

Aquí no interviene Faker. Son millones de filas y todo se construye con numpy
vectorizado: fechas, cantidades e importes como arrays.

Los importes se calculan en **céntimos enteros**, de modo que las invariantes
del modelo se cumplen por construcción y no por redondeo afortunado:

    line_total   = quantity * unit_price - discount_amount
    gross_amount = SUM(line_total + discount_amount)
    total_amount = gross_amount - discount_amount
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from ecommerce.generator import serialize
from ecommerce.generator.dimensions import GeneratedEntity

# Estados de una orden, con su peso. La mayoría llega a destino; la cola son
# los casos que hacen interesante el análisis.
ESTADOS = np.array(["delivered", "shipped", "paid", "pending", "cancelled", "returned"])
PESO_ESTADOS = np.array([0.62, 0.12, 0.10, 0.06, 0.06, 0.04])

METODOS_PAGO = np.array(["credit_card", "debit_card", "paypal", "bank_transfer", "wallet"])
PESO_METODOS = np.array([0.44, 0.24, 0.18, 0.08, 0.06])

# Artículos por orden. La mayoría de las cestas son pequeñas.
ARTICULOS_POR_ORDEN = np.array([1, 2, 3, 4, 5])
PESO_ARTICULOS = np.array([0.45, 0.25, 0.15, 0.10, 0.05])

UNIDADES = np.array([1, 2, 3])
PESO_UNIDADES = np.array([0.72, 0.20, 0.08])

# Proporción de líneas con descuento y su rango.
PROB_DESCUENTO = 0.22
DESCUENTO_MIN, DESCUENTO_MAX = 0.05, 0.30


@dataclass(frozen=True)
class ChunkInternals:
    """Valores internos de un trozo, para las entidades que derivan de él.

    Pagos, reseñas y devoluciones se construyen a partir de las órdenes, y
    necesitan los datos **antes** de convertirse a texto: sumar días a una
    fecha o comparar importes sobre cadenas sería absurdo y frágil.

    Se derivan del dato limpio, antes de inyectar suciedad, porque en la
    realidad son sistemas de origen distintos: el error de un extracto no se
    propaga al de otro.
    """

    ids_orden: np.ndarray
    ids_cliente: np.ndarray
    fechas: np.ndarray
    estados: np.ndarray
    metodos_pago: np.ndarray
    total_cents: np.ndarray
    # Artículos por orden y desplazamiento de la primera línea de cada orden.
    por_orden: np.ndarray
    cortes: np.ndarray
    ids_linea: np.ndarray
    ids_producto_linea: np.ndarray
    unidades: np.ndarray
    total_linea_cents: np.ndarray


@dataclass(frozen=True)
class MarketContext:
    """Lo que se mantiene constante entre trozos.

    Las propensiones de compra y la popularidad de producto se calculan una
    sola vez para todo el lote. Si se recalcularan por trozo, el mismo cliente
    sería fiel en un fichero e inactivo en el siguiente, y la segmentación de
    Gold no tendría sentido.
    """

    customers: GeneratedEntity
    products: GeneratedEntity
    customer_cdf: np.ndarray
    product_cdf: np.ndarray
    inicio: np.datetime64
    dias: int


def build_market_context(
    customers: GeneratedEntity,
    products: GeneratedEntity,
    rng: np.random.Generator,
    inicio: np.datetime64,
    dias: int,
) -> MarketContext:
    """Asigna propensiones lognormales a clientes y productos.

    Una distribución uniforme daría a todos los clientes el mismo número de
    compras, y entonces el valor de vida del cliente y la segmentación RFM
    serían planos: no habría nada que segmentar.
    """
    n_clientes = len(customers.columns["customer_id"])
    n_productos = len(products.columns["product_id"])

    return MarketContext(
        customers=customers,
        products=products,
        customer_cdf=serialize.build_cdf(rng.lognormal(0.0, 1.1, n_clientes)),
        product_cdf=serialize.build_cdf(rng.lognormal(0.0, 1.3, n_productos)),
        inicio=inicio,
        dias=dias,
    )


def generate_orders_chunk(
    contexto: MarketContext,
    n_orders: int,
    order_offset: int,
    item_offset: int,
    rng: np.random.Generator,
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray], ChunkInternals]:
    """Genera un trozo de órdenes con sus líneas.

    Los desplazamientos garantizan identificadores únicos entre trozos: un
    contador reiniciado en cada fichero produciría claves repetidas, y el MERGE
    de Silver machacaría filas distintas entre sí.
    """
    idx_cliente = serialize.weighted_index(contexto.customer_cdf, rng, n_orders)
    ids_cliente = contexto.customers.columns["customer_id"][idx_cliente]
    ids_orden = serialize.ids("ORD", order_offset, n_orders, 9)

    # Sesgo hacia fechas recientes: una beta asimétrica reproduce el
    # crecimiento del negocio y da al dashboard una tendencia real que mostrar,
    # en vez de una línea plana con ruido.
    fraccion = rng.beta(2.0, 1.2, n_orders)
    segundos = (fraccion * contexto.dias * 86_400).astype("timedelta64[s]")
    fechas = contexto.inicio + segundos

    # --- Líneas ---
    por_orden = rng.choice(ARTICULOS_POR_ORDEN, size=n_orders, p=PESO_ARTICULOS)
    n_items = int(por_orden.sum())

    # Cada orden se repite tantas veces como líneas tenga: así la relación
    # línea -> orden queda establecida de forma vectorizada.
    orden_de_linea = np.repeat(ids_orden, por_orden)

    idx_producto = serialize.weighted_index(contexto.product_cdf, rng, n_items)
    unidades = rng.choice(UNIDADES, size=n_items, p=PESO_UNIDADES)
    precio_unitario = contexto.products.numeric["price_cents"][idx_producto]

    bruto_linea = unidades * precio_unitario
    tiene_descuento = rng.random(n_items) < PROB_DESCUENTO
    porcentaje = rng.uniform(DESCUENTO_MIN, DESCUENTO_MAX, n_items)
    descuento_linea = np.where(tiene_descuento, (bruto_linea * porcentaje).astype(np.int64), 0)
    total_linea = bruto_linea - descuento_linea

    # --- Agregación a cabecera ---
    # `add.reduceat` suma por bloques contiguos sin construir un índice
    # intermedio, que es lo que haría un groupby.
    cortes = np.concatenate([[0], np.cumsum(por_orden)[:-1]])
    bruto_orden = np.add.reduceat(bruto_linea, cortes)
    descuento_orden = np.add.reduceat(descuento_linea, cortes)
    total_orden = bruto_orden - descuento_orden

    estados = rng.choice(ESTADOS, size=n_orders, p=PESO_ESTADOS)
    metodos_pago = rng.choice(METODOS_PAGO, size=n_orders, p=PESO_METODOS)

    orders = {
        "order_id": ids_orden,
        "customer_id": ids_cliente.astype(str),
        "order_date": serialize.timestamps_to_str(fechas),
        "status": estados,
        "payment_method": metodos_pago,
        # El país de envío se toma del cliente: una incoherencia aquí sería
        # suciedad no declarada, y toda la suciedad de este proyecto es
        # intencionada y contabilizada.
        "shipping_country": contexto.customers.columns["country"][idx_cliente].astype(str),
        "gross_amount": serialize.money_to_str(bruto_orden),
        "discount_amount": serialize.money_to_str(descuento_orden),
        "total_amount": serialize.money_to_str(total_orden),
        "created_at": serialize.timestamps_to_str(fechas),
        # En el lote inicial nada ha mutado todavía, así que coincide con la
        # creación. Las mutaciones llegan en la Fase 4.
        "updated_at": serialize.timestamps_to_str(fechas),
    }

    ids_linea = serialize.ids("ITM", item_offset, n_items, 10)
    ids_producto_linea = contexto.products.columns["product_id"][idx_producto].astype(str)

    order_items = {
        "order_item_id": ids_linea,
        "order_id": orden_de_linea.astype(str),
        "product_id": ids_producto_linea,
        "quantity": serialize.ints_to_str(unidades),
        "unit_price": serialize.money_to_str(precio_unitario),
        "discount_amount": serialize.money_to_str(descuento_linea),
        "line_total": serialize.money_to_str(total_linea),
    }

    internos = ChunkInternals(
        ids_orden=ids_orden,
        ids_cliente=ids_cliente.astype(str),
        fechas=fechas,
        estados=estados,
        metodos_pago=metodos_pago,
        total_cents=total_orden,
        por_orden=por_orden,
        cortes=cortes,
        ids_linea=ids_linea,
        ids_producto_linea=ids_producto_linea,
        unidades=unidades,
        total_linea_cents=total_linea,
    )

    return orders, order_items, internos
