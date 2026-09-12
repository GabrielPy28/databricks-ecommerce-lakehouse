"""Generación de las dimensiones: clientes y productos.

Faker se usa solo aquí, y ni siquiera fila a fila. Es Python puro y ronda las
decenas de miles de filas por segundo, así que se emplea para construir un
**repertorio** de nombres, ciudades y marcas realistas, del que después se
muestrea de forma vectorizada. El perfil `full` son 200.000 clientes: llamar a
Faker una vez por cliente costaría minutos; muestrear de un repertorio de unos
miles, milisegundos, y el resultado es indistinguible.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from faker import Faker

from ecommerce.generator import serialize

# Tamaño del repertorio del que se muestrea. Suficiente variedad para que los
# datos parezcan reales sin pagar el coste de generar todo con Faker.
POOL = 2_000

# Mercados del negocio simulado, con peso relativo. Alimenta el desglose
# geográfico del dashboard.
PAISES = np.array(["US", "MX", "BR", "ES", "AR", "CO", "CL", "PE"])
PESO_PAISES = np.array([0.34, 0.18, 0.14, 0.12, 0.08, 0.06, 0.05, 0.03])

TAXONOMIA = {
    "Electronics": ["Smartphones", "Laptops", "Audio", "Wearables"],
    "Home": ["Kitchen", "Furniture", "Decor", "Bedding"],
    "Fashion": ["Shoes", "Shirts", "Accessories", "Outerwear"],
    "Sports": ["Fitness", "Outdoor", "Cycling", "Team Sports"],
    "Beauty": ["Skincare", "Fragrance", "Makeup", "Haircare"],
}


@dataclass(frozen=True)
class GeneratedEntity:
    """Una entidad generada.

    `columns` ya está en texto, listo para aterrizar. `numeric` guarda los
    valores internos que otros generadores necesitan —por ejemplo el precio en
    céntimos— para no tener que reconvertir texto a número más adelante.
    """

    columns: dict[str, np.ndarray]
    numeric: dict[str, np.ndarray] = field(default_factory=dict)


def _repertorio(faker: Faker, cuantos: int, generador: str) -> np.ndarray:
    return np.array([getattr(faker, generador)() for _ in range(cuantos)], dtype=object)


def generate_customers(n: int, rng: np.random.Generator, faker: Faker) -> GeneratedEntity:
    nombres = _repertorio(faker, min(POOL, n), "first_name")
    apellidos = _repertorio(faker, min(POOL, n), "last_name")
    ciudades = _repertorio(faker, min(POOL, n), "city")

    primero = nombres[rng.integers(0, len(nombres), n)]
    segundo = apellidos[rng.integers(0, len(apellidos), n)]
    ids = serialize.ids("CUS", 0, n, 7)

    # El índice entra en el correo para garantizar unicidad: con un repertorio
    # de nombres acotado, `nombre.apellido@...` colisionaría constantemente.
    correos = np.array(
        [
            f"{a}.{b}.{i}@example.com".lower()
            for a, b, i in zip(primero, segundo, range(n), strict=True)
        ],
        dtype=object,
    )

    # Antigüedad repartida sobre cinco años hacia atrás desde el inicio del
    # histórico, para que existan clientes veteranos y recién llegados.
    from ecommerce.config import HISTORY_MONTHS, REFERENCE_DATE

    fin = np.datetime64(REFERENCE_DATE) - np.timedelta64(HISTORY_MONTHS * 30, "D")
    altas = fin - rng.integers(0, 5 * 365, n).astype("timedelta64[D]")

    return GeneratedEntity(
        columns={
            "customer_id": ids,
            "first_name": primero.astype(str),
            "last_name": segundo.astype(str),
            "email": correos.astype(str),
            "country": rng.choice(PAISES, size=n, p=PESO_PAISES),
            "city": ciudades[rng.integers(0, len(ciudades), n)].astype(str),
            "registration_date": serialize.dates_to_str(altas),
            # `updated_at` del snapshot inicial es la fecha de **alta**, no la
            # de extracción.
            #
            # Es lo que hace posible unir un hecho con la versión de la
            # dimensión vigente en su fecha: marcándolo con la fecha de
            # extracción, la primera versión solo sería válida desde hoy y
            # ninguna orden del histórico encontraría versión, con lo que el
            # país y el coste saldrían nulos en Gold para casi todo el pasado.
            "updated_at": serialize.timestamps_to_str(altas),
        }
    )


def generate_products(n: int, rng: np.random.Generator, faker: Faker) -> GeneratedEntity:
    marcas = _repertorio(faker, min(POOL // 4, max(n, 1)), "company")

    categorias = np.array(list(TAXONOMIA))
    idx_categoria = rng.integers(0, len(categorias), n)
    cat = categorias[idx_categoria]
    sub = np.array(
        [TAXONOMIA[c][rng.integers(0, len(TAXONOMIA[c]))] for c in cat],
        dtype=object,
    )

    # Distribución lognormal: muchos artículos baratos y una cola de caros, que
    # es como se comporta un catálogo real. Acotada para evitar extremos absurdos.
    precios = np.clip(rng.lognormal(mean=3.6, sigma=0.9, size=n) * 100, 199, 500_000)
    precio_cents = precios.astype(np.int64)

    # Margen bruto entre el 25 % y el 60 %.
    margen = rng.uniform(0.40, 0.75, n)
    coste_cents = (precio_cents * margen).astype(np.int64)

    # El catálogo existe desde el inicio del histórico. Igual que con los
    # clientes, marcar el snapshot inicial con la fecha de extracción dejaría
    # sin versión vigente a todas las ventas anteriores.
    from ecommerce.config import HISTORY_MONTHS, REFERENCE_DATE

    inicio_catalogo = np.datetime64(REFERENCE_DATE, "s") - np.timedelta64(HISTORY_MONTHS * 30, "D")

    return GeneratedEntity(
        columns={
            "product_id": serialize.ids("PRD", 0, n, 6),
            "product_name": np.array(
                [
                    f"{b} {s}"
                    for b, s in zip(marcas[rng.integers(0, len(marcas), n)], sub, strict=True)
                ],
                dtype=object,
            ).astype(str),
            "category": cat.astype(str),
            "subcategory": sub.astype(str),
            "brand": marcas[rng.integers(0, len(marcas), n)].astype(str),
            "price": serialize.money_to_str(precio_cents),
            "cost": serialize.money_to_str(coste_cents),
            # Un 5 % descatalogado: da material para reglas de negocio y para
            # el historial SCD2 de la Fase 4.
            "is_active": np.where(rng.random(n) < 0.05, "false", "true"),
            "updated_at": serialize.timestamps_to_str(np.full(n, inicio_catalogo)),
        },
        numeric={"price_cents": precio_cents},
    )
