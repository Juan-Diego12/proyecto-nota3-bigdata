"""
Proyecto Nota 3 - Big Data | Generacion de muestras para escalabilidad
Crea muestras del 1 %, 10 % y 50 % de cada mes a partir de data/raw.
El 100 % es la carpeta data/raw tal cual (no se copia, para no duplicar ~2 GB).

Uso (desde la raiz del repo, despues de descargar_datos.py):
    python scripts/generar_muestras.py
    python scripts/generar_muestras.py --origen data/raw --destino data/muestras --porcentajes 1 10 50

Resultado:
    data/muestras/p01/yellow_tripdata_AAAA-MM.parquet
    data/muestras/p10/...
    data/muestras/p50/...
    data/muestras/resumen_muestras.csv   (filas por mes y por muestra)

Carpetas para los pipelines (--input):
    1 %   -> data/muestras/p01
    10 %  -> data/muestras/p10
    50 %  -> data/muestras/p50
    100 % -> data/raw

Decisiones:
    - Se procesa archivo por archivo con pyarrow: nunca hay mas de un mes en memoria
      (~3 millones de filas), asi el script corre en cualquier PC.
    - Muestreo sin reemplazo, por mes (estratificado): cada muestra conserva la
      proporcion de viajes de cada mes y anio.
    - Semilla fija 42 combinada con el nombre del mes: todos los integrantes
      obtienen exactamente las mismas filas.
    - Se conservan todas las columnas originales: los pipelines de pandas y Spark
      hacen la seleccion de columnas, igual que con los datos completos.
"""
import argparse
import csv
import zlib
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

SEMILLA = 42


def muestrear(archivo: Path, porcentaje: int, destino: Path) -> tuple[int, int]:
    tabla = pq.read_table(archivo)
    n_total = tabla.num_rows
    n_muestra = round(n_total * porcentaje / 100)

    # Semilla distinta por mes y porcentaje, pero siempre la misma en todos los PC
    # (crc32 del nombre es estable; hash() de Python no lo es entre ejecuciones).
    rng = np.random.default_rng([SEMILLA, zlib.crc32(archivo.name.encode()), porcentaje])
    indices = np.sort(rng.choice(n_total, size=n_muestra, replace=False))

    muestra = tabla.take(indices)
    pq.write_table(muestra, destino / archivo.name, compression="snappy")
    return n_total, n_muestra


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera muestras de 1/10/50 % por mes")
    parser.add_argument("--origen", default="data/raw")
    parser.add_argument("--destino", default="data/muestras")
    parser.add_argument("--porcentajes", nargs="+", type=int, default=[1, 10, 50])
    args = parser.parse_args()

    origen = Path(args.origen)
    archivos = sorted(origen.glob("*.parquet"))
    if not archivos:
        raise SystemExit(f"No hay archivos .parquet en {origen}. Corra primero descargar_datos.py")

    base = Path(args.destino)
    resumen = []
    for porcentaje in args.porcentajes:
        carpeta = base / f"p{porcentaje:02d}"
        carpeta.mkdir(parents=True, exist_ok=True)
        print(f"\n=== Muestra {porcentaje} % -> {carpeta}")
        total_filas = 0
        for i, archivo in enumerate(archivos, start=1):
            n_total, n_muestra = muestrear(archivo, porcentaje, carpeta)
            total_filas += n_muestra
            resumen.append([porcentaje, archivo.name, n_total, n_muestra])
            print(f"  [{i:2d}/{len(archivos)}] {archivo.name}: {n_muestra:,} de {n_total:,} filas")
        tamanio = sum(f.stat().st_size for f in carpeta.glob("*.parquet")) / 1e6
        print(f"  Total: {total_filas:,} filas, {tamanio:,.0f} MB")

    with open(base / "resumen_muestras.csv", "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(["porcentaje", "archivo", "filas_originales", "filas_muestra"])
        escritor.writerows(resumen)

    print(f"\nResumen guardado en {base / 'resumen_muestras.csv'}")
    print("El 100 % corresponde a la carpeta", origen)


if __name__ == "__main__":
    main()
