"""
Proyecto Nota 3 - Big Data | Conjunto ampliado al 200 %
Crea data/x2 con el conjunto completo (data/raw) replicado dos veces, para simular
el crecimiento de los datos y llevar a pandas mas alla de la capacidad del equipo.

Uso (desde la raiz del repo, despues de descargar_datos.py):
    python scripts/replicar_datos.py
    python scripts/replicar_datos.py --origen data/raw --destino data/x2 --copias 2

Resultado:
    data/x2/yellow_tripdata_AAAA-MM_c1.parquet  (copia 1 de cada mes)
    data/x2/yellow_tripdata_AAAA-MM_c2.parquet  (copia 2 de cada mes)
    72 archivos, ~238 millones de filas, ~3,9 GB en disco.

Decisiones:
    - Se replica el conjunto real en lugar de inventar datos: cada copia conserva
      exactamente las distribuciones, el esquema y la calidad de los datos originales.
    - Los archivos se copian (no se enlazan) para que cada lectura sea real desde disco.
    - Ambos pipelines procesan los 72 archivos sin cambios en su codigo; los conteos
      y sumas de R1, R2 y R3 resultan multiplicados por el numero de copias y los
      promedios no cambian.
"""
import argparse
import shutil
from pathlib import Path

import pyarrow.parquet as pq


def main() -> None:
    parser = argparse.ArgumentParser(description="Replica data/raw para crear el 200 %")
    parser.add_argument("--origen", default="data/raw")
    parser.add_argument("--destino", default="data/x2")
    parser.add_argument("--copias", type=int, default=2)
    args = parser.parse_args()

    origen, destino = Path(args.origen), Path(args.destino)
    archivos = sorted(origen.glob("*.parquet"))
    if not archivos:
        raise SystemExit(f"No hay archivos .parquet en {origen}. Corra primero descargar_datos.py")
    destino.mkdir(parents=True, exist_ok=True)

    total_filas, total_bytes = 0, 0
    for copia in range(1, args.copias + 1):
        for i, archivo in enumerate(archivos, start=1):
            nuevo = destino / f"{archivo.stem}_c{copia}.parquet"
            if not (nuevo.exists() and nuevo.stat().st_size == archivo.stat().st_size):
                shutil.copyfile(archivo, nuevo)
            total_filas += pq.ParquetFile(nuevo).metadata.num_rows
            total_bytes += nuevo.stat().st_size
            print(f"  copia {copia} [{i:2d}/{len(archivos)}] {nuevo.name}")

    print(f"\nArchivos en {destino}: {len(archivos) * args.copias}")
    print(f"Filas totales: {total_filas:,}")
    print(f"Tamanio en disco: {total_bytes / 1e9:.2f} GB")


if __name__ == "__main__":
    main()
