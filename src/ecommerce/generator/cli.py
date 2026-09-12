"""Interfaz de línea de comandos del generador.

    python -m ecommerce.generator.cli --profile dev --seed 42 --out data/batches

Escribe en el sistema de ficheros local. La subida al Volume de Databricks es
un paso aparte (Fase 2): el contenedor no tiene montado `/Volumes`, y separar
generación de publicación permite inspeccionar un lote antes de subirlo.
"""

from __future__ import annotations

import argparse
import sys

from ecommerce import config
from ecommerce.generator import DEFAULT_CHUNK_ORDERS, generate_batch
from ecommerce.generator.dirt import DirtRates


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ecommerce-generate",
        description="Genera un lote de datos sintéticos de e-commerce.",
    )
    parser.add_argument(
        "--profile",
        default="dev",
        help=f"Perfil de escala: {', '.join(config.SCALE_PROFILES)} (por defecto: dev)",
    )
    parser.add_argument("--batch", type=int, default=0, help="Número de lote (por defecto: 0)")
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Semilla. La misma semilla produce exactamente el mismo lote.",
    )
    parser.add_argument("--out", required=True, help="Directorio destino")
    parser.add_argument(
        "--chunk-orders",
        type=int,
        default=DEFAULT_CHUNK_ORDERS,
        help="Órdenes por fichero. Acota la memoria en los perfiles grandes.",
    )
    parser.add_argument(
        "--limpio",
        action="store_true",
        help="Genera sin anomalías. Útil para aislar fallos del pipeline de fallos del dato.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)

    # Se resuelve el perfil antes de tocar el disco: un nombre equivocado debe
    # fallar sin dejar un lote a medias.
    try:
        perfil = config.get_profile(args.profile)
    except ValueError as error:
        raise SystemExit(str(error)) from None

    manifiesto = generate_batch(
        profile=perfil,
        batch=args.batch,
        seed=args.seed,
        output_root=args.out,
        dirt_rates=DirtRates.limpio() if args.limpio else None,
        chunk_orders=args.chunk_orders,
    )

    print(f"Lote {config.batch_label(args.batch)} · perfil {perfil.name} · semilla {args.seed}")
    for entidad, filas in manifiesto["rows"].items():
        anomalias = manifiesto["injected"].get(entidad, {})
        detalle = ", ".join(f"{k}={v}" for k, v in anomalias.items() if v) or "sin anomalías"
        print(f"  {entidad:<14} {filas:>12,} filas   ({detalle})")
    print(f"\nManifiesto: {args.out}/_manifests/{config.batch_label(args.batch)}.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
