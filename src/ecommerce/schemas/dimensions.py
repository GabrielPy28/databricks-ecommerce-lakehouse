"""Esquemas lógicos de las dimensiones.

Contrato *tipado*: describe lo que Silver debe producir, no lo que llega en el
fichero crudo. Los ficheros de aterrizaje traen todo como texto (ver
`to_landing_schema`).
"""

from __future__ import annotations

import pyarrow as pa

# El dinero se modela como decimal, nunca como coma flotante: `float` acumula
# error de redondeo y hace que un total no cuadre con la suma de sus líneas por
# unos céntimos, un fallo caro de diagnosticar.
MONEY = pa.decimal128(10, 2)

CUSTOMERS = pa.schema(
    [
        pa.field("customer_id", pa.string(), nullable=False),
        pa.field("first_name", pa.string()),
        pa.field("last_name", pa.string()),
        pa.field("email", pa.string()),
        # ISO-3166 alfa-2. Se conserva para segmentación geográfica; el
        # proyecto trabaja en una sola divisa (USD), así que no implica FX.
        pa.field("country", pa.string()),
        pa.field("city", pa.string()),
        pa.field("registration_date", pa.date32()),
        # Alimenta SCD Tipo 2 en Silver: un cambio de país cierra el registro
        # vigente y abre uno nuevo.
        pa.field("updated_at", pa.timestamp("us")),
    ]
)

PRODUCTS = pa.schema(
    [
        pa.field("product_id", pa.string(), nullable=False),
        # Ausente en el documento original pese a que `gold.product_performance`
        # lo requería.
        pa.field("product_name", pa.string()),
        pa.field("category", pa.string()),
        pa.field("subcategory", pa.string()),
        pa.field("brand", pa.string()),
        pa.field("price", MONEY),
        # Necesario para calcular margen en Gold.
        pa.field("cost", MONEY),
        pa.field("is_active", pa.bool_()),
        pa.field("updated_at", pa.timestamp("us")),
    ]
)
