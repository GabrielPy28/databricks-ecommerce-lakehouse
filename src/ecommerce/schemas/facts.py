"""Esquemas lógicos de los hechos.

Invariantes del modelo (verificadas en los tests del generador):

    orders.total_amount   = gross_amount - discount_amount
    orders.gross_amount   = SUM(order_items.line_total + order_items.discount_amount)
    order_items.line_total = quantity * unit_price - discount_amount
"""

from __future__ import annotations

import pyarrow as pa

# Los importes de cabecera agregan varias líneas, así que necesitan más dígitos
# que un precio unitario.
MONEY = pa.decimal128(10, 2)
MONEY_TOTAL = pa.decimal128(12, 2)

ORDERS = pa.schema(
    [
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("customer_id", pa.string()),
        pa.field("order_date", pa.timestamp("us")),
        pa.field("status", pa.string()),
        pa.field("payment_method", pa.string()),
        pa.field("shipping_country", pa.string()),
        pa.field("gross_amount", MONEY_TOTAL),
        pa.field("discount_amount", MONEY_TOTAL),
        pa.field("total_amount", MONEY_TOTAL),
        pa.field("created_at", pa.timestamp("us")),
        # El campo que sostiene todo el procesamiento incremental. Una orden
        # muta (pending -> paid -> shipped -> delivered) y se reemite; sin
        # `updated_at` no hay forma de saber qué versión es la buena, y el
        # MERGE degenera en un append que duplica.
        pa.field("updated_at", pa.timestamp("us")),
    ]
)

ORDER_ITEMS = pa.schema(
    [
        pa.field("order_item_id", pa.string(), nullable=False),
        pa.field("order_id", pa.string()),
        pa.field("product_id", pa.string()),
        pa.field("quantity", pa.int32()),
        # Precio en el momento de la venta. Nunca se recalcula desde
        # products.price: si lo hiciéramos, un cambio de precio reescribiría
        # retroactivamente los ingresos históricos.
        pa.field("unit_price", MONEY),
        pa.field("discount_amount", MONEY),
        pa.field("line_total", MONEY_TOTAL),
    ]
)

# Un pago sí muta: pending -> completed -> refunded, o failed tras un reintento.
# Por eso lleva `updated_at` y Silver lo trata con MERGE de actualización.
PAYMENTS = pa.schema(
    [
        pa.field("payment_id", pa.string(), nullable=False),
        pa.field("order_id", pa.string()),
        pa.field("payment_method", pa.string()),
        pa.field("amount", MONEY_TOTAL),
        pa.field("payment_status", pa.string()),
        pa.field("payment_date", pa.timestamp("us")),
        pa.field("updated_at", pa.timestamp("us")),
    ]
)

# Las tres entidades siguientes son hechos inmutables: una devolución ya
# tramitada, una reseña ya escrita y un clic ya ocurrido no cambian. No llevan
# `updated_at` a propósito —tenerlo sugeriría lo contrario e invitaría a
# escribir lógica de actualización que sobra— y Silver las trata como
# solo-inserción, desempatando por su propia clave.

RETURNS = pa.schema(
    [
        pa.field("return_id", pa.string(), nullable=False),
        pa.field("order_id", pa.string()),
        # El grano es la línea, no el pedido: se devuelve un artículo concreto.
        pa.field("order_item_id", pa.string()),
        pa.field("product_id", pa.string()),
        pa.field("quantity", pa.int32()),
        pa.field("reason", pa.string()),
        pa.field("refund_amount", MONEY_TOTAL),
        pa.field("return_date", pa.timestamp("us")),
    ]
)

REVIEWS = pa.schema(
    [
        pa.field("review_id", pa.string(), nullable=False),
        pa.field("order_id", pa.string()),
        pa.field("product_id", pa.string()),
        pa.field("customer_id", pa.string()),
        pa.field("rating", pa.int32()),
        pa.field("review_date", pa.timestamp("us")),
    ]
)

WEB_EVENTS = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_timestamp", pa.timestamp("us")),
        pa.field("session_id", pa.string()),
        # Nulo a propósito: la mayor parte del tráfico de un e-commerce no ha
        # iniciado sesión. Exigir cliente obligaría a inventarlos y falsearía
        # por completo la parte alta del embudo.
        pa.field("customer_id", pa.string(), nullable=True),
        pa.field("event_type", pa.string()),
        # Nulo en los eventos que no miran un producto, como ver la portada.
        pa.field("product_id", pa.string(), nullable=True),
        pa.field("device", pa.string()),
        pa.field("utm_source", pa.string()),
    ]
)
