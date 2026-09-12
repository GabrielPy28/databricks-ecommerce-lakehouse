"""Contrato de datos del proyecto: fuente de verdad única.

Lo comparten el generador (que produce los ficheros), la ingesta (que los lee)
y los tests (que verifican que nadie rompa el contrato en silencio).

Hay dos representaciones del mismo contrato:

* **Lógica** (`CUSTOMERS`, `ORDERS`, ...): tipada. Es lo que Silver debe
  producir y lo que Gold consume.
* **De aterrizaje** (`to_landing_schema`): todo texto. Es como llegan los
  ficheros crudos.

La distinción no es un capricho. Una columna Parquet tipada como `timestamp`
no admite una fecha inválida, así que un generador que emitiera datos ya
tipados haría imposible inyectar la suciedad que Silver debe saber manejar.
Además, es como llega de verdad un extracto de un sistema operacional.
"""

from __future__ import annotations

import pyarrow as pa

from ecommerce.schemas.dimensions import CUSTOMERS, PRODUCTS
from ecommerce.schemas.facts import (
    ORDER_ITEMS,
    ORDERS,
    PAYMENTS,
    RETURNS,
    REVIEWS,
    WEB_EVENTS,
)

__all__ = [
    "ALL_SCHEMAS",
    "CUSTOMERS",
    "ORDERS",
    "ORDER_BY",
    "ORDER_ITEMS",
    "PAYMENTS",
    "PRIMARY_KEYS",
    "PRODUCTS",
    "RETURNS",
    "REVIEWS",
    "WEB_EVENTS",
    "to_landing_schema",
]

ALL_SCHEMAS: dict[str, pa.Schema] = {
    "customers": CUSTOMERS,
    "products": PRODUCTS,
    "orders": ORDERS,
    "order_items": ORDER_ITEMS,
    "payments": PAYMENTS,
    "reviews": REVIEWS,
    "returns": RETURNS,
    "web_events": WEB_EVENTS,
}

# Clave de negocio de cada entidad. Es sobre la que Silver hace MERGE, así que
# vive en el contrato y no enterrada en el código de transformación.
PRIMARY_KEYS: dict[str, str] = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "reviews": "review_id",
    "returns": "return_id",
    "web_events": "event_id",
}

# Columna que decide qué versión de una fila es la buena cuando el MERGE
# encuentra la clave repetida.
#
# Las entidades que mutan desempatan por `updated_at`. Las inmutables lo hacen
# por su propia clave, lo que en la práctica las convierte en solo-inserción:
# la condición `s.clave > t.clave` nunca se cumple entre filas ya emparejadas
# por esa misma clave, así que una fila existente jamás se sobrescribe.
ORDER_BY: dict[str, str] = {
    "customers": "updated_at",
    "products": "updated_at",
    "orders": "updated_at",
    "payments": "updated_at",
    # Hechos inmutables.
    "order_items": "order_item_id",
    "reviews": "review_id",
    "returns": "return_id",
    "web_events": "event_id",
}


def to_landing_schema(schema: pa.Schema) -> pa.Schema:
    """Convierte un esquema lógico en su equivalente de aterrizaje.

    Todas las columnas pasan a texto y todas admiten nulos: un fichero crudo
    puede traer cualquier cosa en cualquier columna, incluida la ausencia de
    valor en una que el contrato lógico marca como obligatoria. Rechazarlo es
    trabajo de Silver, no del formato.

    Conserva nombres y orden para que el fichero crudo se corresponda columna a
    columna con su contrato lógico.
    """
    return pa.schema([pa.field(campo.name, pa.string(), nullable=True) for campo in schema])
