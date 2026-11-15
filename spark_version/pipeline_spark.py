"""
Proyecto Nota 3 - Big Data | Version PySpark (v2: optimizaciones + Spark SQL)
Pipeline NYC Yellow Taxi 2022-2024:
    lectura -> limpieza -> transformaciones -> join (broadcast) -> agregaciones -> escritura

Uso (desde la raiz del repo):
    python spark_version/pipeline_spark.py --input data/muestras/p01 --zones data/taxi_zone_lookup.csv

Carpetas de entrada:
    1 %  -> data/muestras/p01      10 % -> data/muestras/p10
    50 % -> data/muestras/p50      100 % -> data/raw

Argumentos principales:
    --input        carpeta con los .parquet mensuales (busca tambien en subcarpetas)
    --zones        ruta de taxi_zone_lookup.csv
    --output       carpeta de salida (r1, r2, r3 en Parquet + r1.csv, r2.csv, r3.csv)
    --api          df (DataFrames, por defecto) o sql (las mismas consultas en Spark SQL)
    --memoria      memoria del driver (por defecto 8g, pensado para un PC de 16 GB)
    --tmp          carpeta para los temporales de Spark (spark.local.dir)
    --particiones  particiones de los shuffles (spark.sql.shuffle.partitions, por defecto 16)

Argumentos para medir el efecto de cada optimizacion (seccion 4 del informe):
    --sin-cache      no guarda en cache la tabla base
    --sin-broadcast  desactiva el broadcast join (Spark usa sort-merge join con shuffle)
    --explain        imprime el plan fisico de las 3 consultas
    --pausa          espera Enter antes de cerrar, para revisar el DAG en http://localhost:4040
"""
import argparse
import glob
import os
import shutil
import time
from collections import defaultdict
from datetime import date
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Columnas que se leen de cada mes y el tipo al que se fuerzan.
# Se fuerza el tipo porque entre meses/anios algunas columnas cambian
# (por ejemplo passenger_count viene como double en unos archivos y como int en otros).
COLUMNAS = {
    "tpep_pickup_datetime": TimestampType(),
    "tpep_dropoff_datetime": TimestampType(),
    "passenger_count": IntegerType(),
    "trip_distance": DoubleType(),
    "pulocationid": IntegerType(),
    "dolocationid": IntegerType(),
    "payment_type": IntegerType(),
    "fare_amount": DoubleType(),
    "tip_amount": DoubleType(),
    "total_amount": DoubleType(),
}

# Columnas que realmente usan las agregaciones. Solo estas se guardan en cache
# (poda de columnas): el cache ocupa menos memoria y disco.
COLUMNAS_BASE = [
    "borough", "zone", "anio", "mes", "dia_semana", "hora", "tipo_pago",
    "total_amount", "fare_amount", "tip_amount", "trip_distance",
    "duracion_min", "velocidad_mph",
]

FECHA_INICIO = date(2022, 1, 1)
FECHA_FIN = date(2024, 12, 31)


# ---------------------------------------------------------------------------
# 0. Sesion de Spark
# ---------------------------------------------------------------------------
def crear_spark(args) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")  # modo local: una tarea por nucleo logico
        .appName(f"NotaTres-PySpark-{args.api}")
        # Por defecto el driver tiene 1 GB; en modo local el driver tambien ejecuta las tareas.
        .config("spark.driver.memory", args.memoria)
        # Zona horaria fija: horas y duraciones iguales a pandas (fechas sin zona horaria).
        .config("spark.sql.session.timeZone", "UTC")
        # Particionamiento: el valor por defecto (200) crea demasiadas tareas pequenias
        # para un PC de 8 hilos; los resultados agregados caben en pocas particiones.
        .config("spark.sql.shuffle.partitions", str(args.particiones))
        # Adaptive Query Execution: ajusta particiones y joins con estadisticas reales.
        .config("spark.sql.adaptive.enabled", "true")
    )
    if args.tmp:
        # Temporales (shuffle y cache que no cabe en memoria) en otra carpeta/disco.
        Path(args.tmp).mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.local.dir", Path(args.tmp).resolve().as_posix())
    if args.sin_broadcast:
        # Sin esto Spark haria broadcast automatico (la tabla de zonas pesa < 10 MB).
        builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------------------
# 1. Lectura
# ---------------------------------------------------------------------------
def agrupar_por_esquema(archivos: list) -> list:
    """Agrupa los archivos que tienen el mismo nombre y tipo en las columnas usadas.

    Leer cada grupo con UNA sola llamada a spark.read.parquet es mas eficiente que leer
    archivo por archivo: el plan tiene menos ramas de union y Spark reparte mejor las tareas.
    Solo se leen los metadatos (footer) de cada Parquet, no los datos.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return [[a] for a in archivos]

    grupos = defaultdict(list)
    for archivo in archivos:
        esquema = pq.read_schema(archivo)
        clave = tuple(
            sorted((campo.name, str(campo.type)) for campo in esquema if campo.name.lower() in COLUMNAS)
        )
        grupos[clave].append(archivo)
    return list(grupos.values())


def leer_viajes(spark: SparkSession, carpeta: str) -> DataFrame:
    archivos = sorted(glob.glob(os.path.join(carpeta, "**", "*.parquet"), recursive=True))
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos .parquet en {carpeta}")
    grupos = agrupar_por_esquema(archivos)
    print(f"Archivos de viajes: {len(archivos)} (agrupados en {len(grupos)} lecturas por esquema)")

    partes = []
    for grupo in grupos:
        df = spark.read.parquet(*[Path(a).resolve().as_posix() for a in grupo])
        df = df.toDF(*[c.lower() for c in df.columns])  # entre anios cambian mayusculas
        # Proyeccion temprana: Parquet es columnar, asi que solo se leen del disco
        # las 10 columnas seleccionadas (column pruning). Los filtros de la limpieza
        # tambien se empujan al lector de Parquet (predicate pushdown).
        partes.append(df.select([F.col(c).cast(t).alias(c) for c, t in COLUMNAS.items()]))

    # unionByName es perezosa: aqui todavia no se lee ningun dato, solo se arma el DAG.
    return reduce(DataFrame.unionByName, partes)


def leer_zonas(spark: SparkSession, ruta: str) -> DataFrame:
    esquema = StructType(
        [
            StructField("locationid", IntegerType()),
            StructField("borough", StringType()),
            StructField("zone", StringType()),
            StructField("service_zone", StringType()),
        ]
    )
    return (
        spark.read.csv(Path(ruta).resolve().as_posix(), header=True, schema=esquema)
        .select("locationid", "borough", "zone")
    )


# ---------------------------------------------------------------------------
# 2. Limpieza
# ---------------------------------------------------------------------------
def limpiar(df: DataFrame) -> DataFrame:
    df = df.dropna(subset=list(COLUMNAS))

    # duracion_min se calcula aqui porque se necesita para el filtro de duracion.
    # cast("double") de un timestamp = segundos (con fraccion) desde 1970.
    df = df.withColumn(
        "duracion_min",
        (F.col("tpep_dropoff_datetime").cast("double") - F.col("tpep_pickup_datetime").cast("double")) / 60.0,
    )

    fecha_recogida = F.to_date("tpep_pickup_datetime")
    return df.filter(
        fecha_recogida.between(F.lit(FECHA_INICIO), F.lit(FECHA_FIN))
        & F.col("duracion_min").between(1, 180)
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= 100)
        & (F.col("fare_amount") > 0)
        & (F.col("total_amount") > 0)
        & F.col("passenger_count").between(1, 6)
    )


# ---------------------------------------------------------------------------
# 3. Transformaciones
# ---------------------------------------------------------------------------
def transformar(df: DataFrame) -> DataFrame:
    pickup = F.col("tpep_pickup_datetime")
    return (
        df.withColumn("velocidad_mph", F.col("trip_distance") / (F.col("duracion_min") / 60.0))
        .withColumn("anio", F.year(pickup))
        .withColumn("mes", F.month(pickup))
        .withColumn("hora", F.hour(pickup))
        # Spark: dayofweek 1 = domingo ... 7 = sabado.
        # Convencion del proyecto: 1 = lunes ... 7 = domingo.
        .withColumn("dia_semana", ((F.dayofweek(pickup) + 5) % 7) + 1)
        .withColumn(
            "tipo_pago",
            F.when(F.col("payment_type") == 1, "Tarjeta")
            .when(F.col("payment_type") == 2, "Efectivo")
            .when(F.col("payment_type") == 3, "Sin cargo")
            .when(F.col("payment_type") == 4, "Disputa")
            .otherwise("Otro"),
        )
    )


# ---------------------------------------------------------------------------
# 4. Join + 5. Agregaciones con la API de DataFrames
# ---------------------------------------------------------------------------
def base_df(viajes: DataFrame, zonas: DataFrame, broadcast: bool) -> DataFrame:
    # La tabla de zonas tiene 265 filas: con broadcast se copia entera a cada tarea
    # y se evita el shuffle de los ~119 millones de viajes (sin broadcast: sort-merge join).
    zonas_join = F.broadcast(zonas) if broadcast else zonas
    unido = viajes.join(zonas_join, viajes["pulocationid"] == zonas["locationid"], "inner")
    return unido.select(COLUMNAS_BASE)


def agregar_df(base: DataFrame) -> dict:
    r1 = (
        base.groupBy("borough", "anio", "mes")
        .agg(
            F.count("*").alias("num_viajes"),
            F.sum("total_amount").alias("ingreso_total"),
            F.avg("fare_amount").alias("tarifa_promedio"),
            F.avg("trip_distance").alias("distancia_promedio"),
            F.avg("duracion_min").alias("duracion_promedio"),
        )
        .orderBy("borough", "anio", "mes")
    )

    r2 = (
        base.groupBy("dia_semana", "hora")
        .agg(
            F.count("*").alias("num_viajes"),
            F.avg("velocidad_mph").alias("velocidad_promedio"),
        )
        .orderBy("dia_semana", "hora")
    )

    r3 = (
        base.groupBy("zone", "borough")
        .agg(
            F.count("*").alias("num_viajes"),
            F.sum("total_amount").alias("ingreso_total"),
            # Propina promedio solo de pagos con tarjeta: avg ignora los nulos del when.
            F.avg(F.when(F.col("tipo_pago") == "Tarjeta", F.col("tip_amount"))).alias("propina_promedio"),
        )
        .orderBy(F.desc("ingreso_total"))
        .limit(20)
    )
    return {"r1": r1, "r2": r2, "r3": r3}


# ---------------------------------------------------------------------------
# 4. Join + 5. Agregaciones con Spark SQL (mismo resultado, mismo motor Catalyst)
# ---------------------------------------------------------------------------
def base_sql(spark: SparkSession, viajes: DataFrame, zonas: DataFrame, broadcast: bool) -> DataFrame:
    viajes.createOrReplaceTempView("viajes")
    zonas.createOrReplaceTempView("zonas")
    pista = "/*+ BROADCAST(z) */" if broadcast else ""
    return spark.sql(f"""
        SELECT {pista}
               z.borough, z.zone, v.anio, v.mes, v.dia_semana, v.hora, v.tipo_pago,
               v.total_amount, v.fare_amount, v.tip_amount, v.trip_distance,
               v.duracion_min, v.velocidad_mph
        FROM viajes v
        JOIN zonas z ON v.pulocationid = z.locationid
    """)


SQL_R1 = """
    SELECT borough, anio, mes,
           COUNT(*)           AS num_viajes,
           SUM(total_amount)  AS ingreso_total,
           AVG(fare_amount)   AS tarifa_promedio,
           AVG(trip_distance) AS distancia_promedio,
           AVG(duracion_min)  AS duracion_promedio
    FROM base
    GROUP BY borough, anio, mes
    ORDER BY borough, anio, mes
"""

SQL_R2 = """
    SELECT dia_semana, hora,
           COUNT(*)           AS num_viajes,
           AVG(velocidad_mph) AS velocidad_promedio
    FROM base
    GROUP BY dia_semana, hora
    ORDER BY dia_semana, hora
"""

SQL_R3 = """
    SELECT zone, borough,
           COUNT(*)          AS num_viajes,
           SUM(total_amount) AS ingreso_total,
           AVG(CASE WHEN tipo_pago = 'Tarjeta' THEN tip_amount END) AS propina_promedio
    FROM base
    GROUP BY zone, borough
    ORDER BY ingreso_total DESC
    LIMIT 20
"""


def agregar_sql(spark: SparkSession, base: DataFrame) -> dict:
    base.createOrReplaceTempView("base")
    return {"r1": spark.sql(SQL_R1), "r2": spark.sql(SQL_R2), "r3": spark.sql(SQL_R3)}


# ---------------------------------------------------------------------------
# 6. Escritura
# ---------------------------------------------------------------------------
def escribir(df: DataFrame, nombre: str, salida: str) -> None:
    destino = Path(salida).resolve()
    destino.mkdir(parents=True, exist_ok=True)

    # El resultado es pequenio: se guarda en cache para no recalcularlo en las dos escrituras.
    df = df.cache()

    # Parquet (formato columnar comprimido), resultado completo sin redondear.
    # coalesce(1): el resultado tiene pocas filas, un solo archivo es suficiente.
    df.coalesce(1).write.mode("overwrite").parquet((destino / nombre).as_posix())

    # CSV para validar contra pandas: decimales redondeados a 2 y un solo archivo.
    # Se pasa a decimal para que Spark no escriba numeros grandes en notacion
    # cientifica (1.67E7), que es como los escribe si quedan como double.
    redondeado = df.select(
        [F.round(c, 2).cast("decimal(20,2)").alias(c) if t == "double" else F.col(c) for c, t in df.dtypes]
    )
    tmp = destino / f"_tmp_{nombre}_csv"
    redondeado.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp.as_posix())
    parte = next(tmp.glob("part-*.csv"))
    os.replace(parte, destino / f"{nombre}.csv")
    shutil.rmtree(tmp, ignore_errors=True)

    df.unpersist()
    print(f"  {nombre}: escrito en {destino / nombre} y {destino / (nombre + '.csv')}")


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline NYC Taxi con PySpark")
    parser.add_argument("--input", required=True, help="Carpeta con los .parquet mensuales")
    parser.add_argument("--zones", required=True, help="Ruta de taxi_zone_lookup.csv")
    parser.add_argument("--output", default="output/spark", help="Carpeta de salida")
    parser.add_argument("--api", choices=["df", "sql"], default="df", help="DataFrames o Spark SQL")
    parser.add_argument("--memoria", default="8g", help="Memoria del driver (ej. 8g)")
    parser.add_argument("--tmp", default=None, help="Carpeta para temporales de Spark")
    parser.add_argument("--particiones", type=int, default=16, help="spark.sql.shuffle.partitions")
    parser.add_argument("--sin-cache", action="store_true", help="No cachear la tabla base")
    parser.add_argument("--sin-broadcast", action="store_true", help="Desactivar broadcast join")
    parser.add_argument("--explain", action="store_true", help="Imprimir los planes fisicos")
    parser.add_argument("--pausa", action="store_true", help="Esperar Enter antes de cerrar (Spark UI)")
    args = parser.parse_args()

    inicio = time.perf_counter()
    spark = crear_spark(args)
    print(
        f"API: {args.api} | cache: {not args.sin_cache} | broadcast: {not args.sin_broadcast} "
        f"| particiones: {args.particiones} | memoria: {args.memoria}"
    )

    # Hasta aqui todo es perezoso (lazy evaluation): Spark solo arma el DAG.
    viajes = transformar(limpiar(leer_viajes(spark, args.input)))
    zonas = leer_zonas(spark, args.zones)

    if args.api == "sql":
        base = base_sql(spark, viajes, zonas, not args.sin_broadcast)
    else:
        base = base_df(viajes, zonas, not args.sin_broadcast)

    # La tabla base se reutiliza en las 3 agregaciones; con cache se lee, limpia y une
    # una sola vez. Nivel por defecto MEMORY_AND_DISK: lo que no cabe en RAM va al disco.
    if not args.sin_cache:
        base = base.cache()

    resultados = agregar_sql(spark, base) if args.api == "sql" else agregar_df(base)

    if args.explain:
        for nombre, df in resultados.items():
            print(f"\n===== Plan fisico de {nombre} =====")
            df.explain(mode="formatted")

    print("Calculando y escribiendo resultados...")
    for nombre, df in resultados.items():
        escribir(df, nombre, args.output)  # cada escritura es una accion: aqui se ejecuta el DAG

    if not args.sin_cache:
        base.unpersist()

    print(f"Tiempo total: {time.perf_counter() - inicio:.1f} s")
    if args.pausa:
        input("Spark UI en http://localhost:4040 . Presiona Enter para terminar...")
    spark.stop()


if __name__ == "__main__":
    main()
