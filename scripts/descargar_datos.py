"""
Proyecto Nota 3 - Big Data | Descarga de datos
Descarga los 36 archivos mensuales de NYC TLC Yellow Taxi (enero 2022 - diciembre 2024)
y la tabla de zonas taxi_zone_lookup.csv desde la pagina oficial de NYC TLC.

Uso (desde la raiz del repo):
    python scripts/descargar_datos.py
    python scripts/descargar_datos.py --destino data --anios 2022 2023 2024

Resultado:
    data/raw/yellow_tripdata_AAAA-MM.parquet   (36 archivos, ~2 GB en total)
    data/taxi_zone_lookup.csv

Caracteristicas:
    - Si un archivo ya existe y es un Parquet valido, no lo vuelve a descargar.
    - Descarga a un archivo .part y solo lo renombra al terminar: si se corta
      la conexion no queda un archivo a medias con el nombre final.
    - Reintenta hasta 3 veces por archivo.
    - Al final verifica cada Parquet y muestra filas y tamanio total.
"""
import argparse
import sys
import time
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq

URL_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{anio}-{mes:02d}.parquet"
URL_ZONAS = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
CABECERAS = {"User-Agent": "Mozilla/5.0 (proyecto academico Big Data - Universidad del Quindio)"}
REINTENTOS = 3


def descargar(url: str, destino: Path) -> None:
    """Descarga url en destino por bloques de 1 MB, mostrando el avance."""
    temporal = destino.with_suffix(destino.suffix + ".part")
    for intento in range(1, REINTENTOS + 1):
        try:
            peticion = urllib.request.Request(url, headers=CABECERAS)
            with urllib.request.urlopen(peticion, timeout=60) as respuesta, open(temporal, "wb") as salida:
                total = int(respuesta.headers.get("Content-Length", 0))
                bajado = 0
                while bloque := respuesta.read(1024 * 1024):
                    salida.write(bloque)
                    bajado += len(bloque)
                    if total:
                        print(f"\r    {bajado / 1e6:6.1f} / {total / 1e6:6.1f} MB", end="", flush=True)
            print()
            temporal.replace(destino)
            return
        except Exception as error:  # red caida, timeout, 403, etc.
            print(f"\n    Error (intento {intento}/{REINTENTOS}): {error}")
            temporal.unlink(missing_ok=True)
            if intento < REINTENTOS:
                time.sleep(5 * intento)
    raise RuntimeError(f"No se pudo descargar {url}")


def es_parquet_valido(ruta: Path) -> bool:
    """Lee solo los metadatos del Parquet: si el archivo esta corrupto o incompleto, falla."""
    try:
        pq.ParquetFile(ruta).metadata
        return True
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Descarga NYC Yellow Taxi 2022-2024 + zonas")
    parser.add_argument("--destino", default="data", help="Carpeta base de datos (por defecto data)")
    parser.add_argument("--anios", nargs="+", type=int, default=[2022, 2023, 2024])
    args = parser.parse_args()

    base = Path(args.destino)
    carpeta_raw = base / "raw"
    carpeta_raw.mkdir(parents=True, exist_ok=True)

    # 1. Tabla de zonas (pequenia, se usa para el join)
    zonas = base / "taxi_zone_lookup.csv"
    if zonas.exists():
        print(f"[OK] {zonas.name} ya existe")
    else:
        print(f"[..] Descargando {zonas.name}")
        descargar(URL_ZONAS, zonas)

    # 2. Archivos mensuales de viajes
    meses = [(a, m) for a in args.anios for m in range(1, 13)]
    fallidos = []
    for i, (anio, mes) in enumerate(meses, start=1):
        destino = carpeta_raw / f"yellow_tripdata_{anio}-{mes:02d}.parquet"
        etiqueta = f"[{i:2d}/{len(meses)}] {destino.name}"
        if destino.exists() and es_parquet_valido(destino):
            print(f"{etiqueta}: ya existe, se omite")
            continue
        print(f"{etiqueta}: descargando")
        try:
            descargar(URL_BASE.format(anio=anio, mes=mes), destino)
        except RuntimeError as error:
            print(f"    {error}")
            fallidos.append(destino.name)

    # 3. Verificacion final
    print("\nVerificando archivos...")
    filas_total, bytes_total = 0, 0
    for archivo in sorted(carpeta_raw.glob("*.parquet")):
        if not es_parquet_valido(archivo):
            fallidos.append(archivo.name + " (corrupto)")
            continue
        filas_total += pq.ParquetFile(archivo).metadata.num_rows
        bytes_total += archivo.stat().st_size

    print(f"Archivos en {carpeta_raw}: {len(list(carpeta_raw.glob('*.parquet')))}")
    print(f"Filas totales: {filas_total:,}")
    print(f"Tamanio en disco: {bytes_total / 1e9:.2f} GB")
    if fallidos:
        print("\nFALTAN O FALLARON (vuelva a correr el script):")
        for nombre in fallidos:
            print(f"  - {nombre}")
        sys.exit(1)
    print("\nDescarga completa.")


if __name__ == "__main__":
    main()
