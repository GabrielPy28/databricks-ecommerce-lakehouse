"""Generador de datos sintéticos de e-commerce.

Punto de entrada: `generate_batch`.

Estrategia híbrida, según el spec: Faker construye repertorios de nombres y
marcas realistas para las dimensiones (miles de filas), y numpy vectorizado
produce los hechos (millones). Faker fila a fila sobre 5 millones de órdenes
costaría horas; así son segundos.

La generación se trocea para acotar la memoria: el perfil `full` son 5 millones
de órdenes y unos 12 millones de líneas, que no caben cómodamente de una vez.
Cada trozo produce su propio fichero, lo que además deja a Auto Loader varios
ficheros que ingerir en paralelo.

Hay dos caminos:

* **Lote 0**: carga inicial. Todo el histórico y las dimensiones completas.
* **Lote N**: incremental. Órdenes nuevas del día, mutaciones de órdenes
  anteriores, datos tardíos y solo las filas de dimensión que cambiaron.

El generador es una función pura de `(semilla, perfil, lote)`. Para mutar una
orden del pasado, el lote N regenera el histórico de forma determinista en vez
de leerlo de un fichero de estado, que acabaría desincronizado del dato y
volvería irreproducible el proyecto entero.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

from ecommerce import config, schemas
from ecommerce.generator import dirt, manifest, mutations
from ecommerce.generator.derived import generate_payments, generate_returns, generate_reviews
from ecommerce.generator.dimensions import generate_customers, generate_products
from ecommerce.generator.events import generate_web_events
from ecommerce.generator.facts import build_market_context, generate_orders_chunk

__all__ = ["DEFAULT_CHUNK_EVENTS", "DEFAULT_CHUNK_ORDERS", "generate_batch"]

# Órdenes por fichero. 250.000 cabeceras generan unas 600.000 líneas, lo que se
# mantiene holgadamente en memoria mientras produce ficheros de tamaño razonable.
DEFAULT_CHUNK_ORDERS = 250_000

# Los eventos web se trocean aparte porque no derivan de las órdenes y son
# muchos más: 50 millones en el perfil `full`.
DEFAULT_CHUNK_EVENTS = 2_000_000

# Compensaciones de semilla. Cada flujo aleatorio deriva de la semilla base por
# un camino distinto y estable, de modo que añadir un trozo no desplaza las
# tiradas de los demás.
_SEMILLA_DIMENSIONES = 0
_SEMILLA_SUCIEDAD_DIM = 1
_SEMILLA_MERCADO = 2
_SEMILLA_TROZO = 100
_SEMILLA_SUCIEDAD_TROZO = 500
_SEMILLA_DERIVADAS = 1_000
_SEMILLA_EVENTOS = 2_000
# El lote entra en la semilla de las mutaciones para que cada lote incremental
# mute un subconjunto distinto y siga siendo reproducible.
_SEMILLA_MUTACIONES = 10_000
_SEMILLA_CAMBIOS_DIM = 20_000

DERIVADAS = ("payments", "reviews", "returns")


def _a_texto(valores: np.ndarray) -> pa.Array:
    """Convierte una columna a texto de Arrow preservando los nulos.

    Los arrays de tipo `object` se pasan tal cual: contienen `None` para las
    columnas que admiten nulos —el cliente de una sesión anónima, el producto
    de una vista de portada— y pyarrow lo traduce a nulo.

    Aplicarles `.astype(str)` los convertiría en la cadena literal `"None"`,
    un valor que parece dato y no lo es, y que ninguna regla de calidad
    detectaría como ausente.
    """
    if valores.dtype == object:
        return pa.array(valores, type=pa.string())
    return pa.array(valores.astype(str), type=pa.string())


def _escribir(
    columnas: dict[str, np.ndarray], entidad: str, root: Path, lote: int, parte: int
) -> int:
    """Escribe un fichero Parquet con el esquema de aterrizaje (todo texto)."""
    esquema = schemas.to_landing_schema(schemas.ALL_SCHEMAS[entidad])
    tabla = pa.table(
        {nombre: _a_texto(columnas[nombre]) for nombre in esquema.names},
        schema=esquema,
    )
    destino = Path(config.batch_path(str(root), entidad, lote))
    destino.mkdir(parents=True, exist_ok=True)
    pq.write_table(tabla, destino / f"part-{parte:05d}.parquet")
    return tabla.num_rows


def _trozos(total: int, tamano: int) -> list[int]:
    completos, resto = divmod(total, tamano)
    return [tamano] * completos + ([resto] if resto else [])


def _concatenar(bloques: list[dict[str, np.ndarray]]) -> dict[str, np.ndarray]:
    """Une varios bloques de columnas en uno, respetando el ancho de texto."""
    bloques = [b for b in bloques if len(next(iter(b.values()), [])) > 0]
    if not bloques:
        return {}
    # Sin `astype(object)`: numpy ya elige el ancho mayor al unir textos de
    # anchos distintos, y convertir a `object` rompería las operaciones
    # vectorizadas de `np.char` que usa la medición de anomalías.
    return {nombre: np.concatenate([b[nombre] for b in bloques]) for nombre in bloques[0]}


def generate_batch(
    *,
    profile: config.ScaleProfile,
    batch: int,
    seed: int,
    output_root: Path | str,
    dirt_rates: dirt.DirtRates | None = None,
    chunk_orders: int = DEFAULT_CHUNK_ORDERS,
    chunk_events: int = DEFAULT_CHUNK_EVENTS,
) -> dict[str, Any]:
    """Genera un lote completo y devuelve su manifiesto.

    El manifiesto también se escribe a disco, en `_manifests/batch_NNN.json`.
    """
    root = Path(output_root)
    tasas = dirt_rates if dirt_rates is not None else dirt.DirtRates()

    # --- Dimensiones (idénticas en todos los lotes: el generador es puro) ---
    faker = Faker()
    faker.seed_instance(seed)
    rng = np.random.default_rng([seed, _SEMILLA_DIMENSIONES])

    customers = generate_customers(profile.customers, rng, faker)
    products = generate_products(profile.products, rng, faker)

    inicio_historico = np.datetime64(config.REFERENCE_DATE, "s") - np.timedelta64(
        config.HISTORY_MONTHS * 30, "D"
    )
    dias_historico = config.HISTORY_MONTHS * 30
    inicio, dias = mutations.ventana_del_lote(inicio_historico, dias_historico, batch)

    contexto = build_market_context(
        customers,
        products,
        np.random.default_rng([seed, _SEMILLA_MERCADO]),
        inicio_historico,
        dias_historico,
    )
    ids_cliente = customers.columns["customer_id"].copy()

    filas: dict[str, int] = {}
    inyectado: dict[str, dict[str, int]] = {e: {} for e in schemas.ALL_SCHEMAS}

    if batch == 0:
        rng_suciedad = np.random.default_rng([seed, _SEMILLA_SUCIEDAD_DIM])
        dirt.ensuciar_customers(customers.columns, rng_suciedad, tasas)
        filas["customers"] = _escribir(customers.columns, "customers", root, batch, 0)
        filas["products"] = _escribir(products.columns, "products", root, batch, 0)
        inyectado["customers"] = dirt.medir_customers(customers.columns)
        incremental = None
    else:
        # Solo las filas de dimensión que cambiaron. Reenviar la dimensión
        # entera funcionaría, pero manda a Bronze cientos de miles de filas
        # idénticas por un puñado de cambios reales.
        rng_dim = np.random.default_rng([seed, _SEMILLA_CAMBIOS_DIM + batch])
        momento = inicio + np.timedelta64(dias, "D")
        cambios_clientes = mutations.cambiar_customers(customers, rng_dim, momento)
        cambios_productos = mutations.cambiar_products(products, rng_dim, momento)
        filas["customers"] = _escribir(cambios_clientes, "customers", root, batch, 0)
        filas["products"] = _escribir(cambios_productos, "products", root, batch, 0)
        incremental = {"changed_customers": filas["customers"]}

    # --- Órdenes ---
    for entidad in ("orders", "order_items", "web_events", *DERIVADAS):
        filas[entidad] = 0

    if batch == 0:
        resultado = _generar_historico(
            root=root,
            batch=batch,
            seed=seed,
            profile=profile,
            contexto=contexto,
            tasas=tasas,
            chunk_orders=chunk_orders,
            ids_cliente=ids_cliente,
            filas=filas,
            inyectado=inyectado,
        )
        n_ordenes_previas, n_lineas_previas = resultado
    else:
        n_ordenes_previas, n_lineas_previas, mutadas = _recorrer_historico(
            seed=seed,
            profile=profile,
            contexto=contexto,
            chunk_orders=chunk_orders,
            batch=batch,
            inicio_lote=inicio,
        )
        incremental.update(
            _generar_incremental(
                root=root,
                batch=batch,
                seed=seed,
                profile=profile,
                contexto=contexto,
                tasas=tasas,
                inicio=inicio,
                dias=dias,
                offset_orden=n_ordenes_previas,
                offset_linea=n_lineas_previas,
                mutadas=mutadas,
                ids_cliente=ids_cliente,
                filas=filas,
                inyectado=inyectado,
            )
        )

    # --- Eventos web ---
    # El lote incremental trae los eventos de su día, no los del histórico.
    n_eventos = profile.web_events if batch == 0 else int(profile.web_events * _fraccion_diaria())
    desplazamiento = 0 if batch == 0 else profile.web_events + (batch - 1) * n_eventos
    for parte, cuantos in enumerate(_trozos(n_eventos, chunk_events)):
        columnas = generate_web_events(
            n_events=cuantos,
            customers=customers,
            products=products,
            product_cdf=contexto.product_cdf,
            rng=np.random.default_rng([seed, _SEMILLA_EVENTOS + batch * 97 + parte]),
            offset=desplazamiento,
            inicio=inicio,
            dias=dias,
        )
        filas["web_events"] += _escribir(columnas, "web_events", root, batch, parte)
        desplazamiento += cuantos

    documento = manifest.build(
        seed=seed,
        profile=profile.name,
        batch=batch,
        chunk_orders=chunk_orders,
        rates=tasas.as_dict(),
        rows=filas,
        injected=inyectado,
        window={"start": str(inicio), "end": str(inicio + np.timedelta64(dias, "D"))},
        incremental=incremental,
    )
    manifest.write(documento, root, config.batch_label(batch))
    return documento


def _fraccion_diaria() -> float:
    return mutations.FRACCION_ORDENES_NUEVAS


def _generar_historico(
    *, root, batch, seed, profile, contexto, tasas, chunk_orders, ids_cliente, filas, inyectado
) -> tuple[int, int]:
    """Carga inicial: todo el histórico de órdenes con sus entidades derivadas."""
    desplazamiento_orden = desplazamiento_linea = 0
    desplazamientos = dict.fromkeys(DERIVADAS, 0)

    for parte, n_ordenes in enumerate(_trozos(profile.orders, chunk_orders)):
        rng_trozo = np.random.default_rng([seed, _SEMILLA_TROZO + parte])
        orders, order_items, internos = generate_orders_chunk(
            contexto, n_ordenes, desplazamiento_orden, desplazamiento_linea, rng_trozo
        )

        # Pagos, reseñas y devoluciones se derivan ANTES de ensuciar las
        # órdenes: son sistemas de origen distintos, y el error de un extracto
        # no se propaga al de otro.
        rng_derivadas = np.random.default_rng([seed, _SEMILLA_DERIVADAS + parte])
        for entidad, generar in (
            ("payments", generate_payments),
            ("reviews", generate_reviews),
            ("returns", generate_returns),
        ):
            columnas = generar(internos, rng_derivadas, desplazamientos[entidad])
            filas[entidad] += _escribir(columnas, entidad, root, batch, parte)
            desplazamientos[entidad] += len(columnas[schemas.PRIMARY_KEYS[entidad]])

        rng_suciedad = np.random.default_rng([seed, _SEMILLA_SUCIEDAD_TROZO + parte])
        dirt.ensuciar_orders(orders, rng_suciedad, tasas)
        dirt.ensuciar_order_items(order_items, rng_suciedad, tasas)
        orders = dirt.duplicar_filas(
            orders, rng_suciedad, dirt.n_afectadas(tasas.duplicate_orders, n_ordenes)
        )

        filas["orders"] += _escribir(orders, "orders", root, batch, parte)
        filas["order_items"] += _escribir(order_items, "order_items", root, batch, parte)

        for entidad, medidos in (
            ("orders", dirt.medir_orders(orders, ids_cliente)),
            ("order_items", dirt.medir_order_items(order_items)),
        ):
            for anomalia, cuantas in medidos.items():
                inyectado[entidad][anomalia] = inyectado[entidad].get(anomalia, 0) + cuantas

        desplazamiento_orden += n_ordenes
        desplazamiento_linea += len(order_items["order_item_id"])

    return desplazamiento_orden, desplazamiento_linea


def _recorrer_historico(
    *, seed, profile, contexto, chunk_orders, batch, inicio_lote
) -> tuple[int, int, dict[str, np.ndarray]]:
    """Regenera el histórico para muestrear las órdenes que van a mutar.

    No se escribe nada: solo se recorre para (a) saber en qué identificador se
    quedó la secuencia y (b) tomar la muestra que este lote reemite con el
    estado avanzado.
    """
    desplazamiento_orden = desplazamiento_linea = 0
    bloques: list[dict[str, np.ndarray]] = []

    for parte, n_ordenes in enumerate(_trozos(profile.orders, chunk_orders)):
        rng_trozo = np.random.default_rng([seed, _SEMILLA_TROZO + parte])
        orders, order_items, _ = generate_orders_chunk(
            contexto, n_ordenes, desplazamiento_orden, desplazamiento_linea, rng_trozo
        )

        rng_mut = np.random.default_rng([seed, _SEMILLA_MUTACIONES + batch * 89 + parte])
        bloques.append(mutations.mutar_ordenes(orders, rng_mut, inicio_lote))

        desplazamiento_orden += n_ordenes
        desplazamiento_linea += len(order_items["order_item_id"])

    return desplazamiento_orden, desplazamiento_linea, _concatenar(bloques)


def _generar_incremental(
    *,
    root,
    batch,
    seed,
    profile,
    contexto,
    tasas,
    inicio,
    dias,
    offset_orden,
    offset_linea,
    mutadas,
    ids_cliente,
    filas,
    inyectado,
) -> dict[str, int]:
    """Lote incremental: órdenes nuevas del día más las mutaciones del pasado."""
    n_nuevas = max(1, int(profile.orders * mutations.FRACCION_ORDENES_NUEVAS))

    # Las órdenes nuevas se generan en la ventana del lote, no en la histórica.
    contexto_lote = dataclasses.replace(contexto, inicio=inicio, dias=dias)
    rng_trozo = np.random.default_rng([seed, _SEMILLA_TROZO + batch * 991])

    # El desplazamiento arranca donde acabó el histórico más lo que consumieron
    # los lotes anteriores: reiniciar el contador haría que una orden nueva
    # reutilizara la clave de una antigua y el MERGE la sobrescribiría.
    inicio_orden = offset_orden + (batch - 1) * n_nuevas
    orders, order_items, internos = generate_orders_chunk(
        contexto_lote, n_nuevas, inicio_orden, offset_linea + (batch - 1) * n_nuevas * 3, rng_trozo
    )

    rng_tardias = np.random.default_rng([seed, _SEMILLA_MUTACIONES + batch])
    n_tardias = mutations.aplicar_retraso(orders, rng_tardias, inicio)

    rng_derivadas = np.random.default_rng([seed, _SEMILLA_DERIVADAS + batch * 977])
    for entidad, generar in (
        ("payments", generate_payments),
        ("reviews", generate_reviews),
        ("returns", generate_returns),
    ):
        columnas = generar(internos, rng_derivadas, inicio_orden * 3)
        filas[entidad] += _escribir(columnas, entidad, root, batch, 0)

    rng_suciedad = np.random.default_rng([seed, _SEMILLA_SUCIEDAD_TROZO + batch * 983])
    dirt.ensuciar_orders(orders, rng_suciedad, tasas)
    dirt.ensuciar_order_items(order_items, rng_suciedad, tasas)

    # Las mutaciones se añaden después de ensuciar: reemiten filas ya existentes
    # y no deben recibir anomalías nuevas.
    todas = _concatenar([orders, mutadas]) if mutadas else orders

    filas["orders"] = _escribir(todas, "orders", root, batch, 0)
    filas["order_items"] = _escribir(order_items, "order_items", root, batch, 0)

    for entidad, medidos in (
        ("orders", dirt.medir_orders(todas, ids_cliente)),
        ("order_items", dirt.medir_order_items(order_items)),
    ):
        inyectado[entidad] = medidos

    return {
        "new_orders": n_nuevas,
        "mutated_orders": len(mutadas.get("order_id", [])),
        "late_orders": n_tardias,
    }
