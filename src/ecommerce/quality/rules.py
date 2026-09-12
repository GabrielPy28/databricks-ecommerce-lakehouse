"""Declaración y carga de reglas de calidad.

Las reglas viven en `rules.yml`, no en el código. Eso permite responder "¿qué
comprueba este pipeline?" leyendo un fichero de treinta líneas en lugar de
rastrear condiciones repartidas por varios notebooks, y añadir una regla sin
tocar Python.

La validación ocurre **al cargar**. Un error tipográfico en el nombre de una
regla debe detenerse al arrancar, no a mitad del pipeline con media hora de
cómputo ya gastada.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_RULES_PATH = Path(__file__).parent / "rules.yml"

# Reglas que se evalúan fila a fila. Cada una necesita una columna.
REGLAS_POR_FILA = frozenset(
    {"not_null", "unique", "in_set", "min_value", "max_value", "foreign_key", "matches"}
)

# Reglas que miran la tabla entera, no sus filas.
#
# `row_count_delta` detecta lo que ninguna regla por fila puede ver: que
# **falten** datos. Una fila ausente no incumple nada —simplemente no está—, así
# que un pipeline que un día ingiere una fracción de lo habitual pasa todas las
# validaciones y publica cifras silenciosamente incompletas.
REGLAS_DE_TABLA = frozenset({"row_count_delta"})

REGLAS_VALIDAS = REGLAS_POR_FILA | REGLAS_DE_TABLA

# `warn` registra y deja pasar. `quarantine` aparta la fila. `fail` detiene el
# pipeline entero.
SEVERIDADES_VALIDAS = frozenset({"warn", "quarantine", "fail"})

# El valor por defecto es el menos destructivo a propósito: si olvidar
# `severity` acabara en `quarantine`, un descuido de configuración retiraría
# datos buenos en silencio.
SEVERIDAD_POR_DEFECTO = "warn"

# Claves que describen la regla en sí; el resto se pasa como parámetros.
_CLAVES_RESERVADAS = frozenset({"column", "rule", "severity"})


@dataclass(frozen=True)
class Rule:
    table: str
    column: str
    rule: str
    severity: str
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def is_table_level(self) -> bool:
        """¿Mira la tabla entera en lugar de sus filas?"""
        return self.rule in REGLAS_DE_TABLA

    @property
    def label(self) -> str:
        """Identificador que aparece en la cuarentena y en el reporte."""
        return f"{self.column}:{self.rule}" if self.column else self.rule


def load_rules(path: Path | str = DEFAULT_RULES_PATH) -> dict[str, list[Rule]]:
    """Carga las reglas agrupadas por tabla, validándolas."""
    contenido = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}

    cargadas: dict[str, list[Rule]] = {}
    for tabla, declaradas in contenido.items():
        cargadas[tabla] = [_construir(tabla, d) for d in declaradas or []]
    return cargadas


def _construir(tabla: str, declarada: dict[str, Any]) -> Rule:
    nombre = declarada.get("rule")
    if nombre not in REGLAS_VALIDAS:
        disponibles = ", ".join(sorted(REGLAS_VALIDAS))
        raise ValueError(f"{tabla}: regla desconocida '{nombre}'. Disponibles: {disponibles}")

    severidad = declarada.get("severity", SEVERIDAD_POR_DEFECTO)
    if severidad not in SEVERIDADES_VALIDAS:
        disponibles = ", ".join(sorted(SEVERIDADES_VALIDAS))
        raise ValueError(
            f"{tabla}: severidad desconocida '{severidad}'. Disponibles: {disponibles}"
        )

    # La columna se valida al cargar, no al ejecutar. Una regla por fila sin
    # columna el motor la ignoraría en silencio: la tabla parecería validada sin
    # estarlo, que es peor que un error ruidoso.
    columna = declarada.get("column", "")
    if nombre in REGLAS_POR_FILA and not columna:
        raise ValueError(f"{tabla}: la regla '{nombre}' necesita una columna")
    if nombre in REGLAS_DE_TABLA and columna:
        raise ValueError(
            f"{tabla}: la regla '{nombre}' mira la tabla entera y no admite columna "
            f"(se declaró '{columna}')"
        )

    return Rule(
        table=tabla,
        column=columna,
        rule=nombre,
        severity=severidad,
        params={k: v for k, v in declarada.items() if k not in _CLAVES_RESERVADAS},
    )
