"""
Proyecto Nota 3 - Big Data | Version pandas

Pipeline NYC Yellow Taxi 2022-2024:
    lectura -> limpieza -> transformaciones -> join -> agregaciones -> escritura

Uso desde la raiz del repositorio:
    python pandas_version/pipeline_pandas.py --input data/muestras/p01 \
        --zones data/taxi_zone_lookup.csv --output output/pandas

Modos de ejecucion:
    completo  Carga y concatena todos los Parquet antes de procesarlos. Es el modo
              comparable con el enfoque tradicional de pandas y puede agotar la RAM.
    archivos  Procesa un archivo mensual a la vez y combina agregados parciales.
              Es la mitigacion por fragmentos documentada en el informe.
"""

from __future__ import annotations

import argparse
import gc
import shutil
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


COLUMNAS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]

DTYPES_ENTEROS = {
    "passenger_count": "Int8",
    "pulocationid": "Int16",
    "dolocationid": "Int16",
    "payment_type": "Int8",
}

FECHA_INICIO = pd.Timestamp("2022-01-01")
FECHA_FIN_EXCLUSIVA = pd.Timestamp("2025-01-01")


def buscar_archivos(carpeta: str) -> list[Path]:
    archivos = sorted(Path(carpeta).rglob("*.parquet"))
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos .parquet en {carpeta}")
    return archivos


def columnas_reales(archivo: Path) -> tuple[list[str], dict[str, str]]:
    """Resuelve diferencias de mayusculas entre los Parquet mensuales."""
    nombres = pq.read_schema(archivo).names
    por_minuscula = {nombre.lower(): nombre for nombre in nombres}
    faltantes = [col for col in COLUMNAS if col not in por_minuscula]
    if faltantes:
        raise ValueError(f"{archivo.name} no contiene las columnas: {faltantes}")
    reales = [por_minuscula[col] for col in COLUMNAS]
    renombrar = {real: real.lower() for real in reales}
    return reales, renombrar


def leer_archivo(archivo: Path) -> pd.DataFrame:
    reales, renombrar = columnas_reales(archivo)
    df = pd.read_parquet(archivo, columns=reales).rename(columns=renombrar)

    # Fechas y flotantes se mantienen en precision de 64 bits para reproducir Spark Double.
    for col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ("trip_distance", "fare_amount", "tip_amount", "total_amount"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col, dtype in DTYPES_ENTEROS.items():
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
    return df


def leer_completo(archivos: list[Path]) -> pd.DataFrame:
    partes = []
    for i, archivo in enumerate(archivos, start=1):
        print(f"  Leyendo [{i:2d}/{len(archivos)}] {archivo.name}")
        partes.append(leer_archivo(archivo))
    return pd.concat(partes, ignore_index=True, copy=False)


def leer_zonas(ruta: str) -> pd.DataFrame:
    zonas = pd.read_csv(ruta, keep_default_na=False)
    zonas.columns = [col.lower() for col in zonas.columns]
    requeridas = {"locationid", "borough", "zone"}
    faltantes = requeridas - set(zonas.columns)
    if faltantes:
        raise ValueError(f"Al archivo de zonas le faltan columnas: {sorted(faltantes)}")
    zonas = zonas[["locationid", "borough", "zone"]].copy()
    zonas["locationid"] = pd.to_numeric(zonas["locationid"], errors="raise").astype("Int16")
    return zonas


def limpiar_transformar_unir(df: pd.DataFrame, zonas: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=COLUMNAS).copy()
    df["duracion_min"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60.0

    pickup = df["tpep_pickup_datetime"]
    mascara = (
        pickup.ge(FECHA_INICIO)
        & pickup.lt(FECHA_FIN_EXCLUSIVA)
        & df["duracion_min"].between(1, 180, inclusive="both")
        & df["trip_distance"].gt(0)
        & df["trip_distance"].le(100)
        & df["fare_amount"].gt(0)
        & df["total_amount"].gt(0)
        & df["passenger_count"].between(1, 6, inclusive="both")
    )
    df = df.loc[mascara].copy()

    pickup = df["tpep_pickup_datetime"]
    df["velocidad_mph"] = df["trip_distance"] / (df["duracion_min"] / 60.0)
    df["anio"] = pickup.dt.year.astype("int16")
    df["mes"] = pickup.dt.month.astype("int8")
    df["hora"] = pickup.dt.hour.astype("int8")
    df["dia_semana"] = (pickup.dt.dayofweek + 1).astype("int8")

    return df.merge(
        zonas,
        how="inner",
        left_on="pulocationid",
        right_on="locationid",
        sort=False,
        validate="many_to_one",
    )


def agregar(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    r1 = (
        df.groupby(["borough", "anio", "mes"], as_index=False, sort=True)
        .agg(
            num_viajes=("total_amount", "size"),
            ingreso_total=("total_amount", "sum"),
            tarifa_promedio=("fare_amount", "mean"),
            distancia_promedio=("trip_distance", "mean"),
            duracion_promedio=("duracion_min", "mean"),
        )
        .sort_values(["borough", "anio", "mes"], kind="stable")
        .reset_index(drop=True)
    )

    r2 = (
        df.groupby(["dia_semana", "hora"], as_index=False, sort=True)
        .agg(
            num_viajes=("total_amount", "size"),
            velocidad_promedio=("velocidad_mph", "mean"),
        )
        .sort_values(["dia_semana", "hora"], kind="stable")
        .reset_index(drop=True)
    )

    totales = df.groupby(["zone", "borough"], as_index=False, sort=False).agg(
        num_viajes=("total_amount", "size"),
        ingreso_total=("total_amount", "sum"),
    )
    tarjetas = (
        df.loc[df["payment_type"].eq(1)]
        .groupby(["zone", "borough"], as_index=False, sort=False)
        .agg(propina_promedio=("tip_amount", "mean"))
    )
    r3 = (
        totales.merge(tarjetas, on=["zone", "borough"], how="left", sort=False)
        .sort_values("ingreso_total", ascending=False, kind="stable")
        .head(20)
        .reset_index(drop=True)
    )
    return {"r1": r1, "r2": r2, "r3": r3}


def parciales(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Agregados aditivos para combinar archivos sin conservar todos los viajes."""
    r1 = df.groupby(["borough", "anio", "mes"], as_index=False, sort=False).agg(
        num_viajes=("total_amount", "size"),
        ingreso_total=("total_amount", "sum"),
        tarifa_suma=("fare_amount", "sum"),
        distancia_suma=("trip_distance", "sum"),
        duracion_suma=("duracion_min", "sum"),
    )
    r2 = df.groupby(["dia_semana", "hora"], as_index=False, sort=False).agg(
        num_viajes=("total_amount", "size"),
        velocidad_suma=("velocidad_mph", "sum"),
    )
    r3 = df.groupby(["zone", "borough"], as_index=False, sort=False).agg(
        num_viajes=("total_amount", "size"),
        ingreso_total=("total_amount", "sum"),
    )
    tarjetas = df.loc[df["payment_type"].eq(1)].groupby(
        ["zone", "borough"], as_index=False, sort=False
    ).agg(propina_suma=("tip_amount", "sum"), propina_cantidad=("tip_amount", "count"))
    r3 = r3.merge(tarjetas, on=["zone", "borough"], how="left", sort=False)
    r3[["propina_suma", "propina_cantidad"]] = r3[
        ["propina_suma", "propina_cantidad"]
    ].fillna(0)
    return {"r1": r1, "r2": r2, "r3": r3}


def combinar_parciales(partes: list[dict[str, pd.DataFrame]]) -> dict[str, pd.DataFrame]:
    p1 = pd.concat([p["r1"] for p in partes], ignore_index=True)
    p1 = p1.groupby(["borough", "anio", "mes"], as_index=False, sort=True).sum()
    r1 = p1[["borough", "anio", "mes", "num_viajes", "ingreso_total"]].copy()
    r1["tarifa_promedio"] = p1["tarifa_suma"] / p1["num_viajes"]
    r1["distancia_promedio"] = p1["distancia_suma"] / p1["num_viajes"]
    r1["duracion_promedio"] = p1["duracion_suma"] / p1["num_viajes"]
    r1 = r1.sort_values(["borough", "anio", "mes"], kind="stable").reset_index(drop=True)

    p2 = pd.concat([p["r2"] for p in partes], ignore_index=True)
    p2 = p2.groupby(["dia_semana", "hora"], as_index=False, sort=True).sum()
    r2 = p2[["dia_semana", "hora", "num_viajes"]].copy()
    r2["velocidad_promedio"] = p2["velocidad_suma"] / p2["num_viajes"]
    r2 = r2.sort_values(["dia_semana", "hora"], kind="stable").reset_index(drop=True)

    p3 = pd.concat([p["r3"] for p in partes], ignore_index=True)
    p3 = p3.groupby(["zone", "borough"], as_index=False, sort=False).sum()
    p3["propina_promedio"] = p3["propina_suma"] / p3["propina_cantidad"].replace(0, pd.NA)
    r3 = (
        p3[["zone", "borough", "num_viajes", "ingreso_total", "propina_promedio"]]
        .sort_values("ingreso_total", ascending=False, kind="stable")
        .head(20)
        .reset_index(drop=True)
    )
    return {"r1": r1, "r2": r2, "r3": r3}


def procesar_por_archivos(archivos: list[Path], zonas: pd.DataFrame) -> dict[str, pd.DataFrame]:
    acumulados = []
    for i, archivo in enumerate(archivos, start=1):
        print(f"  Procesando [{i:2d}/{len(archivos)}] {archivo.name}")
        df = limpiar_transformar_unir(leer_archivo(archivo), zonas)
        acumulados.append(parciales(df))
        del df
        gc.collect()
    return combinar_parciales(acumulados)


def escribir(resultados: dict[str, pd.DataFrame], salida: str) -> None:
    destino = Path(salida)
    destino.mkdir(parents=True, exist_ok=True)
    for nombre, df in resultados.items():
        carpeta_parquet = destino / nombre
        if carpeta_parquet.exists():
            if carpeta_parquet.is_dir():
                shutil.rmtree(carpeta_parquet)
            else:
                carpeta_parquet.unlink()
        carpeta_parquet.mkdir()
        df.to_parquet(carpeta_parquet / "part-00000.parquet", index=False)

        csv = df.copy()
        columnas_decimales = csv.select_dtypes(include=["floating"]).columns
        csv[columnas_decimales] = csv[columnas_decimales].round(2)
        csv.to_csv(destino / f"{nombre}.csv", index=False, encoding="utf-8")
        print(f"  {nombre}: escrito en {carpeta_parquet} y {destino / (nombre + '.csv')}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline NYC Taxi con pandas")
    parser.add_argument("--input", required=True, help="Carpeta con los .parquet mensuales")
    parser.add_argument("--zones", required=True, help="Ruta de taxi_zone_lookup.csv")
    parser.add_argument("--output", default="output/pandas", help="Carpeta de salida")
    parser.add_argument(
        "--modo",
        choices=["completo", "archivos"],
        default="completo",
        help="Carga completa o mitigacion procesando un archivo mensual a la vez",
    )
    args = parser.parse_args()

    inicio = time.perf_counter()
    archivos = buscar_archivos(args.input)
    zonas = leer_zonas(args.zones)
    print(f"Modo: {args.modo} | archivos: {len(archivos)}")

    if args.modo == "archivos":
        resultados = procesar_por_archivos(archivos, zonas)
    else:
        viajes = leer_completo(archivos)
        print(f"Filas leidas: {len(viajes):,}")
        base = limpiar_transformar_unir(viajes, zonas)
        del viajes
        resultados = agregar(base)
        del base

    print("Escribiendo resultados...")
    escribir(resultados, args.output)
    print(f"Tiempo total: {time.perf_counter() - inicio:.1f} s")


if __name__ == "__main__":
    main()
