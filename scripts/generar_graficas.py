"""
Proyecto Nota 3 - Big Data | Tablas y graficas de resultados
Lee el CSV de mediciones del benchmark (pandas vs Spark) y genera automaticamente
las tablas resumen y las graficas de la seccion 6 del informe.

Uso (desde la raiz del repo):
    python scripts/generar_graficas.py
    python scripts/generar_graficas.py --resultados benchmarks/resultados.csv --salida benchmarks/graficas
    python scripts/generar_graficas.py --ejemplo      (prueba con datos inventados, ver abajo)

Formato esperado del CSV de mediciones (una fila por ejecucion):
    herramienta   pandas | spark
    porcentaje    1 | 10 | 50 | 100   (tambien acepta p01, p10, "10 %", ...)
    repeticion    1 | 2 | 3
    tiempo_s      segundos de la ejecucion completa
    ram_pico_mb   RAM pico en MB (para Spark: Python + proceso java.exe hijo)
    estado        ok | error         (opcional; si falta, se asume ok cuando hay tiempo)
    cpu_promedio  % de CPU promedio  (opcional)
    detalle       texto del error, ej. MemoryError (opcional)
Se aceptan algunos nombres alternativos de columnas (ver ALIAS).

Salidas en --salida:
    resumen.csv                    promedio, desviacion y minimo de las repeticiones
    tabla_resumen.md               la misma tabla lista para copiar
    tiempo_vs_volumen.png          seccion 6.2
    memoria_vs_volumen.png         seccion 6.3
    cpu_vs_volumen.png             seccion 6.3 (solo si el CSV trae cpu_promedio)
    aceleracion_spark.png          seccion 6.4 (cuantas veces Spark es mas rapido)
    optimizaciones_spark.png       seccion 4.3 (si existe benchmarks/optimizaciones_p10.txt)

--ejemplo genera datos INVENTADOS solo para probar el script. Se guardan en
output/graficas_ejemplo (carpeta que no se sube al repo) y cada grafica lleva la
marca "DATOS DE EJEMPLO". Nunca usar esas graficas en el informe.
"""
import argparse
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # sin ventana: solo guarda los PNG
import matplotlib.pyplot as plt
import matplotlib.ticker
import numpy as np
import pandas as pd

ALIAS = {
    "tool": "herramienta", "version": "herramienta", "motor": "herramienta",
    "muestra": "porcentaje", "pct": "porcentaje", "porcentaje_datos": "porcentaje",
    "rep": "repeticion", "corrida": "repeticion",
    "tiempo": "tiempo_s", "tiempo_seg": "tiempo_s", "segundos": "tiempo_s", "time_s": "tiempo_s",
    "ram_mb": "ram_pico_mb", "ram_pico": "ram_pico_mb", "memoria_mb": "ram_pico_mb",
    "memoria_pico_mb": "ram_pico_mb", "pico_ram_mb": "ram_pico_mb",
    "status": "estado", "resultado": "estado",
    "cpu": "cpu_promedio", "cpu_pct": "cpu_promedio", "cpu_promedio_pct": "cpu_promedio",
    "error": "detalle", "mensaje": "detalle",
}

PORCENTAJES = [1, 10, 50, 100]
COLOR = {"pandas": "#2a78d6", "Spark": "#eb6834"}  # paleta validada (contraste y daltonismo)
MARCA = {"pandas": "o", "Spark": "s"}
LINEA = {"pandas": "-", "Spark": "--"}
GRIS = "#6b6b6b"

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "Liberation Sans", "DejaVu Sans"],
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.edgecolor": GRIS,
    "axes.labelcolor": "black",
    "xtick.color": "black",
    "ytick.color": "black",
    "axes.grid": True,
    "axes.grid.axis": "y",
    "grid.color": "#e3e3e3",
    "grid.linewidth": 0.8,
    "legend.frameon": False,
    "savefig.dpi": 200,
    "savefig.bbox": "tight",
})


# ---------------------------------------------------------------------------
# Lectura y normalizacion
# ---------------------------------------------------------------------------
def a_porcentaje(valor) -> int:
    numeros = re.findall(r"\d+(?:\.\d+)?", str(valor))
    if not numeros:
        raise ValueError(f"Porcentaje no reconocido: {valor!r}")
    return int(round(float(numeros[0])))


def a_herramienta(valor) -> str:
    texto = str(valor).lower()
    if "spark" in texto:
        return "Spark"
    if "pandas" in texto:
        return "pandas"
    raise ValueError(f"Herramienta no reconocida: {valor!r} (use pandas o spark)")


def leer_mediciones(ruta: Path) -> pd.DataFrame:
    df = pd.read_csv(ruta)
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={c: ALIAS[c] for c in df.columns if c in ALIAS})

    faltan = {"herramienta", "porcentaje", "tiempo_s", "ram_pico_mb"} - set(df.columns)
    if faltan:
        raise SystemExit(f"Al CSV le faltan columnas: {sorted(faltan)}. Tiene: {list(df.columns)}")

    df["herramienta"] = df["herramienta"].map(a_herramienta)
    df["porcentaje"] = df["porcentaje"].map(a_porcentaje)
    if "repeticion" not in df.columns:
        df["repeticion"] = df.groupby(["herramienta", "porcentaje"]).cumcount() + 1
    for col in ["tiempo_s", "ram_pico_mb", "cpu_promedio"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "estado" in df.columns:
        df["ok"] = df["estado"].astype(str).str.strip().str.lower().isin(["ok", "exito", "éxito", "true", "1"])
    else:
        df["ok"] = df["tiempo_s"].notna()
    if "detalle" not in df.columns:
        df["detalle"] = ""
    df["detalle"] = df["detalle"].fillna("").astype(str)
    return df


def filas_por_porcentaje(ruta_resumen: Path) -> dict:
    """Filas reales de cada muestra, tomadas de data/muestras/resumen_muestras.csv."""
    if not ruta_resumen.exists():
        return {}
    r = pd.read_csv(ruta_resumen)
    filas = r.groupby("porcentaje")["filas_muestra"].sum().to_dict()
    primera = r["porcentaje"].iloc[0]
    filas[100] = int(r.loc[r["porcentaje"] == primera, "filas_originales"].sum())
    return {int(k): int(v) for k, v in filas.items()}


# ---------------------------------------------------------------------------
# Resumen
# ---------------------------------------------------------------------------
def resumir(df: pd.DataFrame, filas: dict) -> pd.DataFrame:
    registros = []
    for (herr, pct), g in df.groupby(["herramienta", "porcentaje"]):
        ok = g[g["ok"]]
        fila = {
            "herramienta": herr,
            "porcentaje": pct,
            "filas": filas.get(pct, np.nan),
            "ejecuciones": len(g),
            "exitosas": len(ok),
            "tiempo_prom_s": ok["tiempo_s"].mean(),
            "tiempo_desv_s": ok["tiempo_s"].std(ddof=1) if len(ok) > 1 else np.nan,
            "tiempo_min_s": ok["tiempo_s"].min(),
            # La RAM se toma de todas las ejecuciones: en las fallidas tambien se midio el pico.
            "ram_pico_prom_mb": g["ram_pico_mb"].mean(),
            "ram_pico_max_mb": g["ram_pico_mb"].max(),
            "error": "; ".join(sorted(set(d for d in g.loc[~g["ok"], "detalle"] if d))) or
                     ("fallo" if len(ok) < len(g) else ""),
        }
        if "cpu_promedio" in df.columns:
            fila["cpu_prom_pct"] = ok["cpu_promedio"].mean()
        if fila["tiempo_prom_s"] and not np.isnan(fila["filas"]) and fila["tiempo_prom_s"] > 0:
            fila["millones_filas_por_s"] = fila["filas"] / fila["tiempo_prom_s"] / 1e6
        registros.append(fila)

    res = pd.DataFrame(registros).sort_values(["porcentaje", "herramienta"]).reset_index(drop=True)

    # Aceleracion: tiempo de pandas / tiempo de Spark (solo donde ambos terminaron)
    piv = res.pivot(index="porcentaje", columns="herramienta", values="tiempo_prom_s")
    if {"pandas", "Spark"} <= set(piv.columns):
        acel = (piv["pandas"] / piv["Spark"]).rename("aceleracion_spark")
        res = res.merge(acel, left_on="porcentaje", right_index=True, how="left")
        res.loc[res["herramienta"] == "pandas", "aceleracion_spark"] = np.nan
    return res


def tabla_markdown(res: pd.DataFrame) -> str:
    def f(v, dec=1):
        return "—" if pd.isna(v) else f"{v:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")

    lineas = [
        "| Muestra | Filas | Herramienta | Exitosas | Tiempo promedio (s) | Desv. (s) | RAM pico (GB) | Aceleración Spark |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for _, r in res.iterrows():
        tiempo = f(r["tiempo_prom_s"]) if r["exitosas"] else f"Falló ({r['error']})"
        lineas.append(
            f"| {r['porcentaje']} % | {f(r['filas'], 0)} | {r['herramienta']} | "
            f"{r['exitosas']}/{r['ejecuciones']} | {tiempo} | {f(r['tiempo_desv_s'])} | "
            f"{f(r['ram_pico_max_mb'] / 1024, 2)} | {f(r.get('aceleracion_spark'), 2) + ' x' if pd.notna(r.get('aceleracion_spark')) else '—'} |"
        )
    return "\n".join(lineas) + "\n"


# ---------------------------------------------------------------------------
# Graficas
# ---------------------------------------------------------------------------
def etiquetas_x(filas: dict) -> list:
    out = []
    for p in PORCENTAJES:
        if p in filas:
            out.append(f"{p} %\n({filas[p] / 1e6:,.1f} M filas)".replace(".", ","))
        else:
            out.append(f"{p} %")
    return out


def coma_decimal(ax, eje="y") -> None:
    """Numeros de los ejes con coma decimal, como en el resto del informe."""
    fmt = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}".replace(".", ","))
    (ax.yaxis if eje == "y" else ax.xaxis).set_major_formatter(fmt)


def marca_ejemplo(fig, ejemplo: bool) -> None:
    if ejemplo:
        fig.text(0.5, 0.5, "DATOS DE EJEMPLO", fontsize=34, color="#c8c8c8", alpha=0.6,
                 ha="center", va="center", rotation=25, zorder=0)


def grafica_por_volumen(res, filas, columna, desv, escala, eje_y, titulo, archivo, ejemplo,
                        linea_ref=None, texto_ref=None, incluir_fallidas=False, extras=None):
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    x_pos = {p: i for i, p in enumerate(PORCENTAJES)}
    techo = 0

    for herr in ["pandas", "Spark"]:
        g = res[res["herramienta"] == herr].set_index("porcentaje")
        if g.empty:
            continue
        validos = g[g["exitosas"] > 0] if not incluir_fallidas else g
        validos = validos[validos[columna].notna()]
        xs = [x_pos[p] for p in validos.index if p in x_pos]
        ys = [validos.loc[p, columna] * escala for p in validos.index if p in x_pos]
        err = None
        if desv and desv in validos:
            err = [0 if pd.isna(validos.loc[p, desv]) else validos.loc[p, desv] * escala
                   for p in validos.index if p in x_pos]
        ax.errorbar(xs, ys, yerr=err, color=COLOR[herr], marker=MARCA[herr], markersize=7,
                    linestyle=LINEA[herr], linewidth=2, capsize=4, label=herr, zorder=3)
        if ys:
            techo = max(techo, max(ys))
            ax.annotate(herr, (xs[-1], ys[-1]), textcoords="offset points", xytext=(8, 0),
                        va="center", fontsize=9, color="black")

        # Ejecuciones que fallaron: X roja de texto negro, nunca solo color.
        fallidas = g[g["exitosas"] == 0]
        for p, r in fallidas.iterrows():
            if p not in x_pos or (incluir_fallidas and pd.notna(r[columna])):
                continue
            y = (techo or 1) * 1.08
            ax.scatter([x_pos[p]], [y], marker="x", s=90, color=COLOR[herr], linewidths=2.5, zorder=4)
            ax.annotate(f"{herr}: falló\n({r['error'] or 'error'})", (x_pos[p], y),
                        textcoords="offset points", xytext=(0, 10), ha="center", fontsize=8.5)
        if incluir_fallidas:
            for p, r in fallidas.iterrows():
                if p in x_pos and pd.notna(r[columna]):
                    ax.annotate(f"falló ({r['error'] or 'error'})", (x_pos[p], r[columna] * escala),
                                textcoords="offset points", xytext=(0, -14), ha="center", va="top",
                                fontsize=8.5)

    # Puntos adicionales, ej. memoria comprometida de pandas (incluye lo paginado a disco),
    # que la memoria residente medida con psutil no refleja.
    for herr, pct, valor, texto in (extras or []):
        if pct not in x_pos:
            continue
        ax.scatter([x_pos[pct]], [valor], marker=MARCA[herr], s=70, facecolors="white",
                   edgecolors=COLOR[herr], linewidths=2, zorder=4)
        ax.plot([x_pos[pct], x_pos[pct]], [res[(res["herramienta"] == herr) & (res["porcentaje"] == pct)][columna].max() * escala, valor],
                color=COLOR[herr], linestyle=":", linewidth=1.5, zorder=2)
        ax.annotate(texto, (x_pos[pct], valor), textcoords="offset points", xytext=(-10, 0),
                    ha="right", va="center", fontsize=8.5)
        techo = max(techo, valor)

    if linea_ref is not None:
        ax.axhline(linea_ref, color=GRIS, linestyle=":", linewidth=1.5, zorder=2)
        ax.annotate(texto_ref, (0, linea_ref), textcoords="offset points", xytext=(0, 5),
                    fontsize=8.5, color="black")
        techo = max(techo, linea_ref)

    ax.set_xticks(range(len(PORCENTAJES)))
    ax.set_xticklabels(etiquetas_x(filas))
    ax.set_xlim(-0.3, len(PORCENTAJES) - 0.4)
    ax.set_ylim(0, techo * 1.3 if techo else 1)
    ax.set_xlabel("Tamaño de la muestra")
    ax.set_ylabel(eje_y)
    ax.set_title(titulo, loc="left", fontsize=11, fontweight="bold")
    ax.legend(loc="upper left")
    coma_decimal(ax)
    marca_ejemplo(fig, ejemplo)
    fig.savefig(archivo)
    plt.close(fig)


def grafica_aceleracion(res, filas, archivo, ejemplo):
    g = res[(res["herramienta"] == "Spark") & res["aceleracion_spark"].notna()]
    if g.empty:
        return False
    fig, ax = plt.subplots(figsize=(7.2, 3.8))
    etiquetas = dict(zip(PORCENTAJES, etiquetas_x(filas)))
    xs = range(len(g))
    ax.bar(xs, g["aceleracion_spark"], color=COLOR["Spark"], width=0.55, zorder=3)
    ax.axhline(1, color=GRIS, linestyle=":", linewidth=1.5, zorder=2)
    ax.set_xlim(-0.5, len(g) - 0.5)
    ax.annotate("1 x = igual de rápido", (-0.45, 1), textcoords="offset points",
                xytext=(0, 5), ha="left", fontsize=8.5)
    for x, v in zip(xs, g["aceleracion_spark"]):
        ax.annotate(f"{v:.2f} x".replace(".", ","), (x, v), textcoords="offset points",
                    xytext=(0, 4), ha="center", fontsize=9)
    ax.set_xticks(list(xs))
    ax.set_xticklabels([etiquetas[p] for p in g["porcentaje"]])
    ax.set_ylabel("Tiempo pandas / tiempo Spark")
    ax.set_title("Aceleración de Spark frente a pandas (mayor que 1 = Spark más rápido)",
                 loc="left", fontsize=11, fontweight="bold")
    ax.set_ylim(0, max(g["aceleracion_spark"].max(), 1) * 1.25)
    coma_decimal(ax)
    marca_ejemplo(fig, ejemplo)
    fig.savefig(archivo)
    plt.close(fig)
    return True


def leer_optimizaciones(ruta: Path) -> list:
    """Lee lineas 'nombre: 32.5 s' de benchmarks/optimizaciones_p10.txt."""
    if not ruta.exists():
        return []
    datos = []
    for linea in ruta.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = re.match(r"\s*(.+?)\s*:\s*([\d.,]+)\s*s\b", linea)
        if m:
            datos.append((m.group(1), float(m.group(2).replace(",", "."))))
    return datos


def grafica_optimizaciones(datos, archivo, titulo):
    fig, ax = plt.subplots(figsize=(7.2, 0.55 * len(datos) + 1.3))
    nombres = [d[0] for d in datos]
    tiempos = [d[1] for d in datos]
    ys = range(len(datos))[::-1]
    ax.barh(list(ys), tiempos, color=COLOR["Spark"], height=0.55, zorder=3)
    for y, t in zip(ys, tiempos):
        ax.annotate(f"{t:.1f} s".replace(".", ","), (t, y), textcoords="offset points",
                    xytext=(4, 0), va="center", fontsize=9)
    ax.set_yticks(list(ys))
    ax.set_yticklabels(nombres)
    ax.grid(axis="x", color="#e3e3e3")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, max(tiempos) * 1.18)
    ax.set_xlabel("Tiempo total (s)")
    ax.set_title(titulo, loc="left", fontsize=11, fontweight="bold")
    fig.savefig(archivo)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Datos de ejemplo (solo para probar el script)
# ---------------------------------------------------------------------------
def crear_ejemplo(ruta: Path) -> None:
    rng = np.random.default_rng(0)
    filas = []
    base = {  # (tiempo s, RAM MB) inventados con la forma esperada
        ("pandas", 1): (6, 900), ("pandas", 10): (45, 5200), ("pandas", 50): (260, 15500),
        ("Spark", 1): (16, 2100), ("Spark", 10): (33, 3900), ("Spark", 50): (110, 6800),
        ("Spark", 100): (210, 7600),
    }
    for (herr, pct), (t, ram) in base.items():
        for rep in (1, 2, 3):
            filas.append([herr.lower(), pct, rep, round(t * rng.normal(1, 0.04), 1),
                          round(ram * rng.normal(1, 0.02)), "ok", round(rng.uniform(15, 95), 1), ""])
    for rep in (1, 2, 3):
        filas.append(["pandas", 100, rep, "", 15800, "error", "", "MemoryError"])
    pd.DataFrame(filas, columns=["herramienta", "porcentaje", "repeticion", "tiempo_s",
                                 "ram_pico_mb", "estado", "cpu_promedio", "detalle"]).to_csv(ruta, index=False)


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Tablas y graficas pandas vs Spark")
    parser.add_argument("--resultados", default="benchmarks/resultados.csv", help="CSV de mediciones")
    parser.add_argument("--salida", default="benchmarks/graficas", help="Carpeta de salida")
    parser.add_argument("--muestras", default="data/muestras/resumen_muestras.csv",
                        help="Resumen de muestras (para poner el numero real de filas)")
    parser.add_argument("--optimizaciones", default="benchmarks/optimizaciones_p10.txt")
    parser.add_argument("--ram-total-gb", type=float, default=15.8,
                        help="RAM utilizable del equipo de prueba (linea de referencia)")
    parser.add_argument("--pandas-comprometida-gb", type=float, default=None,
                        help="Memoria comprometida del sistema durante pandas al 100 %% (RAM + paginacion), en GB")
    parser.add_argument("--ejemplo", action="store_true", help="Probar con datos inventados")
    args = parser.parse_args()

    salida = Path(args.salida)
    resultados = Path(args.resultados)
    if args.ejemplo:
        salida = Path("output/graficas_ejemplo")
        salida.mkdir(parents=True, exist_ok=True)
        resultados = salida / "resultados_EJEMPLO.csv"
        crear_ejemplo(resultados)
        print("MODO EJEMPLO: datos inventados, solo para probar. No usar en el informe.")
    if not resultados.exists():
        raise SystemExit(f"No existe {resultados}. Corra primero el benchmark o use --ejemplo para probar.")
    salida.mkdir(parents=True, exist_ok=True)

    filas = filas_por_porcentaje(Path(args.muestras))
    df = leer_mediciones(resultados)
    res = resumir(df, filas)
    print(f"Mediciones leidas: {len(df)} ejecuciones ({df['ok'].sum()} exitosas)")

    res.to_csv(salida / "resumen.csv", index=False, encoding="utf-8")
    (salida / "tabla_resumen.md").write_text(tabla_markdown(res), encoding="utf-8")

    grafica_por_volumen(res, filas, "tiempo_prom_s", "tiempo_desv_s", 1, "Tiempo de ejecución (s)",
                        "Tiempo de ejecución según el volumen de datos (promedio de 3 repeticiones)",
                        salida / "tiempo_vs_volumen.png", args.ejemplo)
    grafica_por_volumen(res, filas, "ram_pico_max_mb", None, 1 / 1024, "Memoria (GB)",
                        "Memoria RAM pico (residente) según el volumen de datos",
                        salida / "memoria_vs_volumen.png", args.ejemplo,
                        linea_ref=args.ram_total_gb,
                        texto_ref=f"RAM utilizable del equipo: {args.ram_total_gb:g} GB".replace(".", ","),
                        incluir_fallidas=True,
                        extras=[("pandas", 100, args.pandas_comprometida_gb,
                                 f"Memoria comprometida del sistema durante\npandas: {args.pandas_comprometida_gb:g} GB (incluye paginación)".replace(".", ","))]
                        if args.pandas_comprometida_gb else None)
    generadas = ["tiempo_vs_volumen.png", "memoria_vs_volumen.png"]
    if "cpu_prom_pct" in res and res["cpu_prom_pct"].notna().any():
        grafica_por_volumen(res, filas, "cpu_prom_pct", None, 1, "CPU promedio (%)",
                            "Uso promedio de CPU según el volumen de datos",
                            salida / "cpu_vs_volumen.png", args.ejemplo)
        generadas.append("cpu_vs_volumen.png")
    if grafica_aceleracion(res, filas, salida / "aceleracion_spark.png", args.ejemplo):
        generadas.append("aceleracion_spark.png")

    opt = leer_optimizaciones(Path(args.optimizaciones))
    if opt:
        grafica_optimizaciones(opt, salida / "optimizaciones_spark.png",
                               "Efecto de cada optimización de Spark (muestra del 10 %)")
        generadas.append("optimizaciones_spark.png")

    print(f"\nResumen:\n{tabla_markdown(res)}")
    print(f"Archivos en {salida}:")
    for nombre in ["resumen.csv", "tabla_resumen.md", *generadas]:
        print(f"  - {nombre}")


if __name__ == "__main__":
    main()
