"""Lotes incrementales: mutaciones, datos tardíos y cambios de dimensión.

El lote 0 es la carga inicial. Los siguientes traen lo que hace interesante el
procesamiento incremental:

* **Órdenes nuevas**, del día que cubre el lote.
* **Órdenes mutadas**: la misma clave con un estado posterior y una marca de
  actualización más reciente. El MERGE debe actualizarlas, no duplicarlas.
* **Datos tardíos**: registros con fecha de una ventana anterior que llegan
  ahora. No deben revertir el estado actual.
* **Cambios de dimensión**, que alimentan el historial SCD2.

Nada de esto se guarda entre ejecuciones. El generador sigue siendo una función
pura de `(semilla, perfil, lote)`: para mutar una orden del pasado, regenera el
histórico de forma determinista y toma de él la muestra. Cuesta O(histórico)
por lote, pero evita un fichero de estado que acabaría desincronizado del dato
y volviendo irreproducible todo el proyecto.
"""

from __future__ import annotations

import numpy as np

from ecommerce.generator import serialize
from ecommerce.generator.dimensions import GeneratedEntity

# Ciclo de vida de una orden. El índice da el orden: una orden solo avanza.
CICLO = ("pending", "paid", "shipped", "delivered", "returned")
ORDEN_DEL_CICLO = {estado: indice for indice, estado in enumerate(CICLO)}
# `cancelled` es un final alternativo: no encaja en la secuencia, así que se
# trata como terminal y nunca se muta.
ORDEN_DEL_CICLO["cancelled"] = len(CICLO)

ESTADOS_TERMINALES = frozenset({"returned", "cancelled"})

# Proporción de órdenes nuevas por lote, respecto al histórico. Un lote diario
# sobre dos años de historia mueve en torno al 0,2 % diario; se usa algo más
# para que los lotes de demostración tengan volumen visible.
FRACCION_ORDENES_NUEVAS = 0.02

# Proporción del histórico que muta en cada lote.
FRACCION_MUTACIONES = 0.03

# Proporción de las órdenes nuevas que llegan tarde: su fecha pertenece a una
# ventana anterior aunque el registro aparezca ahora.
FRACCION_TARDIAS = 0.10
DIAS_DE_RETRASO_MAX = 5

# Proporción de la dimensión que cambia en cada lote.
FRACCION_CLIENTES_CAMBIAN = 0.02
FRACCION_PRODUCTOS_CAMBIAN = 0.03

# Variación de precio cuando un producto cambia.
VARIACION_PRECIO_MIN, VARIACION_PRECIO_MAX = 0.85, 1.25


def ventana_del_lote(inicio_historico: np.datetime64, dias_historico: int, lote: int):
    """Ventana temporal que cubre un lote.

    El lote 0 abarca todo el histórico; cada lote posterior, un día más.
    """
    fin_historico = inicio_historico + np.timedelta64(dias_historico, "D")
    if lote == 0:
        return inicio_historico, dias_historico
    return fin_historico + np.timedelta64(lote - 1, "D"), 1


def avanzar_estado(estados: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Hace avanzar cada estado un paso del ciclo.

    Los terminales se quedan como están: una orden cancelada no se entrega
    después, y permitirlo produciría historiales imposibles que ninguna regla
    de calidad detectaría porque cada fila, por separado, es válida.
    """
    siguientes = []
    for estado in estados:
        if estado in ESTADOS_TERMINALES:
            siguientes.append(estado)
            continue
        indice = ORDEN_DEL_CICLO[estado]
        # Un pequeño porcentaje salta directamente a devuelta.
        if estado == "delivered":
            siguientes.append("returned" if rng.random() < 0.15 else "delivered")
        else:
            siguientes.append(CICLO[min(indice + 1, len(CICLO) - 1)])
    return np.array(siguientes, dtype=object).astype(str)


def mutar_ordenes(
    columnas: dict[str, np.ndarray], rng: np.random.Generator, momento: np.datetime64
) -> dict[str, np.ndarray]:
    """Reemite un subconjunto de órdenes con el estado avanzado.

    Se conservan cliente, importes y fecha de la orden: una orden que avanza de
    estado sigue siendo la misma orden. Lo único que cambia es `status` y
    `updated_at`, que es exactamente lo que el MERGE necesita para decidir.
    """
    filas = len(columnas["order_id"])
    cuantas = int(filas * FRACCION_MUTACIONES)
    if cuantas == 0:
        return {nombre: valores[:0] for nombre, valores in columnas.items()}

    posiciones = rng.choice(filas, size=cuantas, replace=False)
    mutadas = {nombre: valores[posiciones].copy() for nombre, valores in columnas.items()}

    mutadas["status"] = avanzar_estado(mutadas["status"], rng)
    mutadas["updated_at"] = serialize.timestamps_to_str(
        np.full(cuantas, momento) + rng.integers(0, 86_400, cuantas).astype("timedelta64[s]")
    )
    return mutadas


def _cambiar_dimension(
    entidad: GeneratedEntity,
    rng: np.random.Generator,
    fraccion: float,
    momento: np.datetime64,
    cambiar: callable,
) -> dict[str, np.ndarray]:
    """Reenvía solo las filas de la dimensión que cambiaron.

    Reenviar la dimensión entera en cada lote también funcionaría, pero manda a
    Bronze cientos de miles de filas idénticas por un puñado de cambios reales.
    """
    filas = len(next(iter(entidad.columns.values())))
    cuantas = max(1, int(filas * fraccion))
    posiciones = rng.choice(filas, size=cuantas, replace=False)

    cambiadas = {nombre: valores[posiciones].copy() for nombre, valores in entidad.columns.items()}
    cambiar(cambiadas, rng)
    cambiadas["updated_at"] = serialize.timestamps_to_str(np.full(cuantas, momento))
    return cambiadas


def cambiar_customers(
    customers: GeneratedEntity, rng: np.random.Generator, momento: np.datetime64
) -> dict[str, np.ndarray]:
    """Clientes que se mudan. Es el cambio que justifica SCD2."""

    def mudanza(columnas: dict[str, np.ndarray], rng: np.random.Generator) -> None:
        from ecommerce.generator.dimensions import PAISES

        columnas["country"] = rng.choice(PAISES, size=len(columnas["country"]))

    return _cambiar_dimension(customers, rng, FRACCION_CLIENTES_CAMBIAN, momento, mudanza)


def cambiar_products(
    products: GeneratedEntity, rng: np.random.Generator, momento: np.datetime64
) -> dict[str, np.ndarray]:
    """Productos que cambian de precio o se descatalogan."""

    def reprecio(columnas: dict[str, np.ndarray], rng: np.random.Generator) -> None:
        cuantos = len(columnas["price"])
        # El precio se recalcula desde el texto: es el único sitio del generador
        # donde hay que deshacer la serialización, y sale más barato que
        # arrastrar los céntimos de toda la dimensión hasta aquí.
        centimos = np.array([round(float(p) * 100) for p in columnas["price"]], dtype=np.int64)
        factor = rng.uniform(VARIACION_PRECIO_MIN, VARIACION_PRECIO_MAX, cuantos)
        columnas["price"] = serialize.money_to_str((centimos * factor).astype(np.int64))
        columnas["is_active"] = np.where(rng.random(cuantos) < 0.10, "false", "true")

    return _cambiar_dimension(products, rng, FRACCION_PRODUCTOS_CAMBIAN, momento, reprecio)


def aplicar_retraso(
    columnas: dict[str, np.ndarray], rng: np.random.Generator, inicio_ventana: np.datetime64
) -> int:
    """Retrasa la fecha de algunas órdenes a ventanas anteriores.

    El registro llega en este lote pero ocurrió antes. Es el caso que rompe los
    pipelines que asumen que lo último en llegar es lo más reciente.

    Devuelve cuántas se retrasaron.
    """
    filas = len(columnas["order_id"])
    cuantas = int(filas * FRACCION_TARDIAS)
    if cuantas == 0:
        return 0

    posiciones = rng.choice(filas, size=cuantas, replace=False)
    retraso = rng.integers(1, DIAS_DE_RETRASO_MAX + 1, cuantas).astype("timedelta64[D]")

    fechas = np.array(
        [np.datetime64(f.replace(" ", "T"), "s") for f in columnas["order_date"][posiciones]]
    )
    nuevas = serialize.timestamps_to_str(fechas - retraso)

    fecha_col = columnas["order_date"].astype(f"<U{max(19, nuevas.dtype.itemsize // 4)}")
    fecha_col[posiciones] = nuevas
    columnas["order_date"] = fecha_col
    return cuantas
