"""Compara los CSV producidos por pandas y Spark para R1, R2 y R3."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


CLAVES = {
    "r1": ["borough", "anio", "mes"],
    "r2": ["dia_semana", "hora"],
    "r3": ["zone", "borough"],
}


def cargar(carpeta: Path, nombre: str, claves: list[str]) -> pd.DataFrame:
    ruta = carpeta / f"{nombre}.csv"
    if not ruta.exists():
        raise FileNotFoundError(f"No existe {ruta}")
    df = pd.read_csv(ruta, keep_default_na=False)
    faltantes = set(claves) - set(df.columns)
    if faltantes:
        raise ValueError(f"A {ruta} le faltan columnas clave: {sorted(faltantes)}")
    return df.sort_values(claves, kind="stable").reset_index(drop=True)


def validar_tabla(nombre: str, pandas_dir: Path, spark_dir: Path, tolerancia: float) -> bool:
    claves = CLAVES[nombre]
    izquierda = cargar(pandas_dir, nombre, claves)
    derecha = cargar(spark_dir, nombre, claves)
    errores = []

    if list(izquierda.columns) != list(derecha.columns):
        errores.append(
            f"columnas distintas: pandas={list(izquierda.columns)}, Spark={list(derecha.columns)}"
        )
    if len(izquierda) != len(derecha):
        errores.append(f"filas distintas: pandas={len(izquierda)}, Spark={len(derecha)}")
    if errores:
        print(f"[ERROR] {nombre}: " + " | ".join(errores))
        return False

    for clave in claves:
        if not izquierda[clave].astype(str).equals(derecha[clave].astype(str)):
            errores.append(f"las claves de {clave} no coinciden")

    for col in [c for c in izquierda.columns if c not in claves]:
        p = pd.to_numeric(izquierda[col], errors="coerce").to_numpy(dtype=float)
        s = pd.to_numeric(derecha[col], errors="coerce").to_numpy(dtype=float)
        iguales = np.isclose(p, s, rtol=0, atol=tolerancia, equal_nan=True)
        if not iguales.all():
            diferencias = np.abs(p - s)
            maximo = np.nanmax(diferencias) if np.isfinite(diferencias).any() else float("nan")
            errores.append(
                f"{col}: {int((~iguales).sum())} valores distintos (diferencia maxima {maximo:g})"
            )

    if errores:
        print(f"[ERROR] {nombre}: " + " | ".join(errores))
        return False
    print(f"[OK] {nombre}: {len(izquierda)} filas equivalentes")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Valida resultados pandas frente a Spark")
    parser.add_argument("--pandas", default="output/pandas_p01", help="Salida de pandas")
    parser.add_argument("--spark", default="output/spark_p01", help="Salida de Spark")
    parser.add_argument("--tolerancia", type=float, default=0.011, help="Tolerancia absoluta")
    args = parser.parse_args()

    correctas = [
        validar_tabla(nombre, Path(args.pandas), Path(args.spark), args.tolerancia)
        for nombre in ("r1", "r2", "r3")
    ]
    if not all(correctas):
        raise SystemExit(1)
    print("Validacion completa: pandas y Spark producen resultados equivalentes.")


if __name__ == "__main__":
    main()
