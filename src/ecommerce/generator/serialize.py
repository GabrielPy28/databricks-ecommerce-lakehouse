"""Conversión de arrays numéricos al texto que aterriza en los ficheros.

Todo el dinero se calcula en **céntimos enteros** y solo se convierte a texto al
final. Es una decisión deliberada: con coma flotante, `line_total` y la suma de
sus componentes difieren en céntimos de forma impredecible, y las invariantes
del modelo dejarían de cumplirse exactamente. Con enteros se cumplen por
construcción.
"""

from __future__ import annotations

import numpy as np


def money_to_str(cents: np.ndarray) -> np.ndarray:
    """Céntimos enteros -> texto decimal con dos posiciones. `-1250` -> `'-12.50'`."""
    cents = np.asarray(cents, dtype=np.int64)
    signo = np.where(cents < 0, "-", "")
    absolutos = np.abs(cents)
    enteros = (absolutos // 100).astype(str)
    decimales = np.char.zfill((absolutos % 100).astype(str), 2)
    return np.char.add(signo, np.char.add(enteros, np.char.add(".", decimales)))


def ints_to_str(valores: np.ndarray) -> np.ndarray:
    return np.asarray(valores).astype(str)


def timestamps_to_str(valores: np.ndarray) -> np.ndarray:
    """datetime64 -> `'YYYY-MM-DD HH:MM:SS'`.

    Se usa espacio en lugar de la `T` de ISO porque es el formato que Spark
    parsea sin necesidad de indicarle un patrón explícito.
    """
    texto = np.asarray(valores, dtype="datetime64[s]").astype(str)
    return np.char.replace(texto, "T", " ")


def dates_to_str(valores: np.ndarray) -> np.ndarray:
    """datetime64 -> `'YYYY-MM-DD'`."""
    return np.asarray(valores, dtype="datetime64[D]").astype(str)


def ids(prefijo: str, inicio: int, cantidad: int, ancho: int) -> np.ndarray:
    """Identificadores correlativos: `ids('ORD', 0, 3, 8)` -> ORD-00000000..02.

    El relleno con ceros mantiene el orden lexicográfico alineado con el
    numérico, algo que se agradece al inspeccionar datos a mano.
    """
    numeros = np.arange(inicio, inicio + cantidad)
    return np.char.add(f"{prefijo}-", np.char.zfill(numeros.astype(str), ancho))


def weighted_index(cdf: np.ndarray, rng: np.random.Generator, cantidad: int) -> np.ndarray:
    """Muestreo con pesos, vectorizado.

    `rng.choice(..., p=...)` reconstruye la distribución en cada llamada y se
    vuelve costoso con millones de extracciones. Buscar valores uniformes en
    una función de distribución acumulada precalculada es equivalente y mucho
    más rápido.
    """
    return np.searchsorted(cdf, rng.random(cantidad), side="right").clip(0, len(cdf) - 1)


def build_cdf(pesos: np.ndarray) -> np.ndarray:
    acumulado = np.cumsum(pesos, dtype=np.float64)
    return acumulado / acumulado[-1]
