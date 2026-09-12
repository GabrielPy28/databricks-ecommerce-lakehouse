"""Inyección controlada de anomalías.

Tres principios:

1. **Recuentos exactos, no probabilidades.** El número de filas afectadas es
   `round(tasa * filas)`, no una tirada por fila. Así el manifiesto puede
   afirmar "2.314 registros" y ser exactamente cierto, y los tests comprueban
   igualdad en lugar de tolerancias estadísticas.
2. **El manifiesto se mide, no se estima.** Los recuentos se obtienen
   inspeccionando el resultado final, no sumando lo que cada función pretendía
   hacer. Importa porque las anomalías interactúan: duplicar filas puede copiar
   una que ya era huérfana, y entonces el fichero tendría más huérfanos de los
   inyectados. El manifiesto describe el dato real.
3. **Anchos explícitos.** Los arrays de texto de numpy son de ancho fijo:
   escribir `" DELIVERED "` (11 caracteres) en una columna dimensionada para
   `"delivered"` (9) truncaría el valor sin avisar.

Opera sobre columnas ya convertidas a texto, que es como llega la data cruda de
un sistema operacional y lo único que permite escribir una fecha inválida.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np

# Valores inválidos que aparecen en extractos reales: campos vacíos, marcadores
# de sistemas de origen y fechas sintácticamente imposibles.
FECHAS_INVALIDAS = ("", "N/A", "not-a-date", "0000-00-00 00:00:00", "2026-13-45 99:99:99")

# Rango de identificadores de cliente que nunca se emite, reservado para
# fabricar referencias rotas.
PREFIJO_HUERFANO = "CUS-9"


@dataclass(frozen=True)
class DirtRates:
    """Proporción de filas afectada por cada anomalía. Valores por defecto del spec."""

    duplicate_orders: float = 0.0030
    orphan_customer_id: float = 0.0010
    non_positive_quantity: float = 0.0005
    negative_price: float = 0.0002
    invalid_timestamp: float = 0.0010
    inconsistent_status: float = 0.0200
    malformed_email: float = 0.0050

    @classmethod
    def limpio(cls) -> DirtRates:
        """Sin anomalías. Sirve para aislar fallos del pipeline de los del dato."""
        return cls(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    def as_dict(self) -> dict[str, float]:
        return asdict(self)


def n_afectadas(tasa: float, filas: int) -> int:
    return round(tasa * filas)


def _posiciones(rng: np.random.Generator, filas: int, cuantas: int) -> np.ndarray:
    """Posiciones distintas elegidas al azar, sin reemplazo."""
    if cuantas <= 0:
        return np.empty(0, dtype=np.int64)
    return rng.choice(filas, size=min(cuantas, filas), replace=False)


def _ensanchar(valores: np.ndarray, extra: int) -> np.ndarray:
    """Amplía el ancho de un array de texto para que quepan valores mayores.

    Sin esto, `array_de_9_caracteres[i] = "un_valor_mas_largo"` trunca en
    silencio: no lanza error, simplemente corta. Es una de las formas más
    desagradables de perder datos.
    """
    valores = np.asarray(valores)
    ancho = valores.dtype.itemsize // 4 if valores.dtype.kind == "U" else 32
    return valores.astype(f"<U{ancho + extra}")


# --------------------------------------------------------------------------
# Inyección
# --------------------------------------------------------------------------


def ensuciar_customers(
    columnas: dict[str, np.ndarray], rng: np.random.Generator, tasas: DirtRates
) -> None:
    filas = len(columnas["customer_id"])
    posiciones = _posiciones(rng, filas, n_afectadas(tasas.malformed_email, filas))
    if len(posiciones) == 0:
        return

    # Se quita la arroba en lugar de vaciar el campo: un correo presente pero
    # inválido es más difícil de detectar que uno nulo, y por eso mejor caso de
    # prueba para las reglas de calidad.
    correos = _ensanchar(columnas["email"], 0)
    correos[posiciones] = np.char.replace(correos[posiciones], "@", ".")
    columnas["email"] = correos


def ensuciar_orders(
    columnas: dict[str, np.ndarray], rng: np.random.Generator, tasas: DirtRates
) -> None:
    filas = len(columnas["order_id"])

    # Referencias rotas: identificadores de cliente de un rango que no existe.
    posiciones = _posiciones(rng, filas, n_afectadas(tasas.orphan_customer_id, filas))
    if len(posiciones):
        clientes = _ensanchar(columnas["customer_id"], 4)
        clientes[posiciones] = np.char.add(
            PREFIJO_HUERFANO, np.char.zfill(np.arange(len(posiciones)).astype(str), 6)
        )
        columnas["customer_id"] = clientes

    # Fechas no parseables.
    posiciones = _posiciones(rng, filas, n_afectadas(tasas.invalid_timestamp, filas))
    if len(posiciones):
        fechas = _ensanchar(columnas["order_date"], 4)
        fechas[posiciones] = rng.choice(FECHAS_INVALIDAS, size=len(posiciones))
        columnas["order_date"] = fechas

    # Mayúsculas y espacios inconsistentes: el caso clásico de un sistema de
    # origen sin normalizar. Silver debe unificarlo antes de validar el dominio.
    posiciones = _posiciones(rng, filas, n_afectadas(tasas.inconsistent_status, filas))
    if len(posiciones):
        estados = _ensanchar(columnas["status"], 4)
        plantillas = (" {} ", "{}", " {}", "{} ")
        elegidas = rng.integers(0, len(plantillas), size=len(posiciones))
        estados[posiciones] = [
            plantillas[v].format(estados[p].upper())
            for p, v in zip(posiciones, elegidas, strict=True)
        ]
        columnas["status"] = estados


def ensuciar_order_items(
    columnas: dict[str, np.ndarray], rng: np.random.Generator, tasas: DirtRates
) -> None:
    filas = len(columnas["order_item_id"])

    posiciones = _posiciones(rng, filas, n_afectadas(tasas.non_positive_quantity, filas))
    if len(posiciones):
        cantidades = _ensanchar(columnas["quantity"], 2)
        cantidades[posiciones] = rng.choice(np.array(["0", "-1", "-2"]), size=len(posiciones))
        columnas["quantity"] = cantidades

    # El signo se antepone al texto ya formateado, dejando la fila incoherente
    # respecto a `line_total`: justo lo que la calidad debe detectar.
    posiciones = _posiciones(rng, filas, n_afectadas(tasas.negative_price, filas))
    if len(posiciones) == 0:
        return
    precios = _ensanchar(columnas["unit_price"], 1)
    precios[posiciones] = np.char.add("-", precios[posiciones])
    columnas["unit_price"] = precios


def duplicar_filas(
    columnas: dict[str, np.ndarray], rng: np.random.Generator, cuantas: int
) -> dict[str, np.ndarray]:
    """Reemite filas existentes tal cual.

    Es el escenario real: el sistema de origen entrega dos veces el mismo
    registro. Bronze debe preservarlo y Silver deduplicarlo.
    """
    if cuantas <= 0:
        return columnas
    filas = len(next(iter(columnas.values())))
    posiciones = rng.choice(filas, size=cuantas, replace=True)
    return {
        nombre: np.concatenate([valores, valores[posiciones]])
        for nombre, valores in columnas.items()
    }


# --------------------------------------------------------------------------
# Medición
# --------------------------------------------------------------------------


def medir_customers(columnas: dict[str, np.ndarray]) -> dict[str, int]:
    correos = columnas["email"]
    return {"malformed_email": int(np.sum(np.char.find(correos, "@") < 0))}


def medir_orders(columnas: dict[str, np.ndarray], ids_cliente: np.ndarray) -> dict[str, int]:
    ids = columnas["order_id"]
    # `np.char` no opera sobre arrays de tipo `object`, y las columnas pueden
    # llegar así tras unir bloques. `status` nunca es nulo, así que convertir a
    # texto es seguro aquí.
    estados = np.asarray(columnas["status"], dtype=str)
    return {
        "duplicate_orders": int(len(ids) - len(np.unique(ids))),
        "orphan_customer_id": int(np.sum(~np.isin(columnas["customer_id"], ids_cliente))),
        "invalid_timestamp": int(np.sum(np.isin(columnas["order_date"], FECHAS_INVALIDAS))),
        "inconsistent_status": int(np.sum(np.char.lower(np.char.strip(estados)) != estados)),
    }


def medir_order_items(columnas: dict[str, np.ndarray]) -> dict[str, int]:
    return {
        "non_positive_quantity": int(np.sum(columnas["quantity"].astype(np.int64) <= 0)),
        "negative_price": int(np.sum(np.char.startswith(columnas["unit_price"], "-"))),
    }
