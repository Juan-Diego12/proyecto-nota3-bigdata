"""
Ejecuta los pipelines pandas y Spark, mide tiempo, RAM pico y CPU con psutil,
y agrega una fila por ejecucion a benchmarks/resultados.csv.

La RAM de Spark incluye el proceso Python y todos sus hijos recursivos, incluido java.exe.
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time
from pathlib import Path

import psutil


CAMPOS = [
    "herramienta",
    "porcentaje",
    "repeticion",
    "tiempo_s",
    "ram_pico_mb",
    "estado",
    "cpu_promedio",
    "detalle",
]


def entradas(datos: Path) -> dict[int, Path]:
    return {
        1: datos / "muestras" / "p01",
        10: datos / "muestras" / "p10",
        50: datos / "muestras" / "p50",
        100: datos / "raw",
    }


def procesos_vivos(raiz: psutil.Process) -> list[psutil.Process]:
    procesos = []
    try:
        procesos.append(raiz)
        procesos.extend(raiz.children(recursive=True))
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return procesos


def medir(comando: list[str], log: Path, intervalo: float) -> dict[str, object]:
    log.parent.mkdir(parents=True, exist_ok=True)
    inicio = time.perf_counter()
    ram_pico = 0
    muestras_cpu = []
    monitorizados: dict[int, psutil.Process] = {}
    nucleos = psutil.cpu_count(logical=True) or 1

    try:
        with log.open("w", encoding="utf-8", errors="replace") as salida:
            salida.write("COMANDO: " + subprocess.list2cmdline(comando) + "\n\n")
            salida.flush()
            proceso = subprocess.Popen(
                comando,
                stdout=salida,
                stderr=subprocess.STDOUT,
                cwd=Path(__file__).resolve().parents[1],
            )
            raiz = psutil.Process(proceso.pid)

            while True:
                arbol = procesos_vivos(raiz)
                rss = 0
                cpu = 0.0
                for actual in arbol:
                    try:
                        rss += actual.memory_info().rss
                        if actual.pid not in monitorizados:
                            actual.cpu_percent(None)
                            monitorizados[actual.pid] = actual
                        else:
                            # cpu_percent conserva la lectura anterior en el objeto Process.
                            # Reutilizarlo es esencial para los hijos de Spark (java.exe).
                            cpu += monitorizados[actual.pid].cpu_percent(None)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                ram_pico = max(ram_pico, rss)
                if monitorizados:
                    # psutil entrega 100 % por nucleo; se normaliza a 0-100 % del equipo.
                    muestras_cpu.append(min(cpu / nucleos, 100.0))

                if proceso.poll() is not None:
                    break
                time.sleep(intervalo)

            codigo = proceso.wait()
    except Exception as error:
        return {
            "codigo": None,
            "tiempo": time.perf_counter() - inicio,
            "ram_mb": ram_pico / (1024**2),
            "cpu": sum(muestras_cpu) / len(muestras_cpu) if muestras_cpu else None,
            "detalle": f"{type(error).__name__}: {error}",
        }

    detalle = ""
    if codigo != 0:
        try:
            lineas = [linea.strip() for linea in log.read_text(encoding="utf-8").splitlines() if linea.strip()]
            detalle = " | ".join(lineas[-3:])[-1000:]
        except OSError:
            detalle = f"Codigo de salida {codigo}"
    return {
        "codigo": codigo,
        "tiempo": time.perf_counter() - inicio,
        "ram_mb": ram_pico / (1024**2),
        "cpu": sum(muestras_cpu) / len(muestras_cpu) if muestras_cpu else None,
        "detalle": detalle,
    }


def claves_existentes(ruta: Path) -> set[tuple[str, int, int]]:
    if not ruta.exists():
        return set()
    with ruta.open(newline="", encoding="utf-8-sig") as archivo:
        return {
            (fila["herramienta"].lower(), int(fila["porcentaje"]), int(fila["repeticion"]))
            for fila in csv.DictReader(archivo)
        }


def guardar(ruta: Path, fila: dict[str, object]) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    nuevo = not ruta.exists() or ruta.stat().st_size == 0
    with ruta.open("a", newline="", encoding="utf-8") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=CAMPOS)
        if nuevo:
            escritor.writeheader()
        escritor.writerow(fila)
        archivo.flush()
        os.fsync(archivo.fileno())


def comando_para(
    herramienta: str,
    python: str,
    entrada: Path,
    zonas: Path,
    salida: Path,
    tmp_spark: Path,
    modo_pandas: str,
) -> list[str]:
    if herramienta == "pandas":
        return [
            python,
            "pandas_version/pipeline_pandas.py",
            "--input",
            str(entrada),
            "--zones",
            str(zonas),
            "--output",
            str(salida),
            "--modo",
            modo_pandas,
        ]
    return [
        python,
        "spark_version/pipeline_spark.py",
        "--input",
        str(entrada),
        "--zones",
        str(zonas),
        "--output",
        str(salida),
        "--tmp",
        str(tmp_spark),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark pandas frente a Spark")
    parser.add_argument("--datos", default="data", help="Carpeta con raw, muestras y zonas")
    parser.add_argument("--resultados", default="benchmarks/resultados.csv")
    parser.add_argument("--tmp-spark", default="output/spark-tmp")
    parser.add_argument("--porcentajes", nargs="+", type=int, default=[1, 10, 50, 100])
    parser.add_argument("--repeticiones", type=int, default=3)
    parser.add_argument("--herramientas", nargs="+", choices=["pandas", "spark"], default=["pandas", "spark"])
    parser.add_argument("--modo-pandas", choices=["completo", "archivos"], default="completo")
    parser.add_argument("--intervalo", type=float, default=0.5)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--reiniciar", action="store_true", help="Repetir claves que ya existen")
    args = parser.parse_args()

    datos = Path(args.datos).resolve()
    zonas = datos / "taxi_zone_lookup.csv"
    mapa = entradas(datos)
    invalidos = set(args.porcentajes) - set(mapa)
    if invalidos:
        raise SystemExit(f"Porcentajes no admitidos: {sorted(invalidos)}")
    if not zonas.exists():
        raise SystemExit(f"No existe {zonas}")
    for porcentaje in args.porcentajes:
        if not mapa[porcentaje].exists():
            raise SystemExit(f"No existe la entrada del {porcentaje} %: {mapa[porcentaje]}")

    resultados = Path(args.resultados)
    if args.reiniciar and resultados.exists():
        resultados.unlink()
    existentes = claves_existentes(resultados)
    total = len(args.herramientas) * len(args.porcentajes) * args.repeticiones
    actual = 0

    for herramienta in args.herramientas:
        for porcentaje in args.porcentajes:
            for repeticion in range(1, args.repeticiones + 1):
                actual += 1
                clave = (herramienta, porcentaje, repeticion)
                if clave in existentes:
                    print(f"[{actual}/{total}] Se omite {clave}: ya existe en {resultados}")
                    continue

                salida = Path("output/benchmark") / f"{herramienta}_p{porcentaje:02d}"
                log = Path("benchmarks/logs") / f"{herramienta}_p{porcentaje:02d}_r{repeticion}.log"
                comando = comando_para(
                    herramienta,
                    args.python,
                    mapa[porcentaje],
                    zonas,
                    salida,
                    Path(args.tmp_spark).resolve(),
                    args.modo_pandas,
                )
                print(f"[{actual}/{total}] {herramienta} {porcentaje} % repeticion {repeticion}")
                medicion = medir(comando, log, args.intervalo)
                ok = medicion["codigo"] == 0
                fila = {
                    "herramienta": herramienta,
                    "porcentaje": porcentaje,
                    "repeticion": repeticion,
                    "tiempo_s": f"{medicion['tiempo']:.3f}",
                    "ram_pico_mb": f"{medicion['ram_mb']:.1f}",
                    "estado": "ok" if ok else "error",
                    "cpu_promedio": "" if medicion["cpu"] is None else f"{medicion['cpu']:.1f}",
                    "detalle": medicion["detalle"],
                }
                guardar(resultados, fila)
                print(
                    f"  estado={fila['estado']} tiempo={fila['tiempo_s']} s "
                    f"RAM={fila['ram_pico_mb']} MB CPU={fila['cpu_promedio'] or 'N/D'} %"
                )
                if not ok:
                    print(f"  detalle: {fila['detalle']} | log: {log}")

    print(f"Benchmark terminado. Resultados en {resultados}")


if __name__ == "__main__":
    main()
