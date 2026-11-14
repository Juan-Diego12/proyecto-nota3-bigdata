"""
Proyecto Nota 3 - Big Data | Version PySpark (v1)
Pipeline NYC Yellow Taxi 2022-2024:
    lectura -> limpieza -> transformaciones -> join (broadcast) -> agregaciones -> escritura

Uso (desde la raiz del repo):
    python spark_version/pipeline_spark.py --input data/dev --zones data/taxi_zone_lookup.csv --output output/spark

Argumentos:
    --input      carpeta con los .parquet mensuales (busca tambien en subcarpetas)
    --zones      ruta de taxi_zone_lookup.csv
    --output     carpeta de salida (se crean r1, r2, r3 en Parquet y r1.csv, r2.csv, r3.csv)
    --memoria    memoria del driver de Spark (por defecto 8g, pensado para un PC de 16 GB)
    --sin-cache  desactiva el cache de la tabla base (para comparar el efecto del cache)
"""
import argparse
import glob
import os
import shutil
import time
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

FECHA_INICIO = date(2022, 1, 1)
FECHA_FIN = date(2024, 12, 31)


# ---------------------------------------------------------------------------
# 0. Sesion de Spark
# ---------------------------------------------------------------------------
def crear_spark(memoria_driver: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")  # modo local: un executor por nucleo logico
        .appName("NotaTres-PySpark")
        # Por defecto el driver tiene 1 GB; en modo local el driver tambien ejecuta las tareas.
        .config("spark.driver.memory", memoria_driver)
        # Zona horaria fija: evita que horas y duraciones cambien por horario de verano
        # y deja los resultados iguales a pandas (que usa fechas sin zona horaria).
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------------------
# 1. Lectura
# ---------------------------------------------------------------------------
def leer_viajes(spark: SparkSession, carpeta: str) -> DataFrame:
    """Lee mes por mes, pasa columnas a minusculas, selecciona y unifica tipos, y une todo."""
    archivos = sorted(glob.glob(os.path.join(carpeta, "**", "*.parquet"), recursive=True))
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos .parquet en {carpeta}")
    print(f"Archivos de viajes encontrados: {len(archivos)}")

    partes = []
    for archivo in archivos:
        df = spark.read.parquet(Path(archivo).resolve().as_posix())
        df = df.toDF(*[c.lower() for c in df.columns])  # entre anios cambian mayusculas
        partes.append(df.select([F.col(c).cast(t).alias(c) for c, t in COLUMNAS.items()]))

    # unionByName es una transformacion perezosa: aqui todavia no se lee ningun dato,
    # solo se construye el plan (DAG).
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
# 4. Join
# ---------------------------------------------------------------------------
def unir_zonas(viajes: DataFrame, zonas: DataFrame) -> DataFrame:
    # La tabla de zonas tiene 265 filas: con broadcast se copia entera a cada tarea
    # y se evita el shuffle de los ~110 millones de viajes.
    return viajes.join(
        F.broadcast(zonas), viajes["pulocationid"] == zonas["locationid"], "inner"
    ).drop("locationid")


# ---------------------------------------------------------------------------
# 5. Agregaciones
# ---------------------------------------------------------------------------
def agregar(base: DataFrame) -> dict:
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
            F.avg(F.when(F.col("payment_type") == 1, F.col("tip_amount"))).alias("propina_promedio"),
        )
        .orderBy(F.desc("ingreso_total"))
        .limit(20)
    )
    return {"r1": r1, "r2": r2, "r3": r3}


# ---------------------------------------------------------------------------
# 6. Escritura
# ---------------------------------------------------------------------------
def escribir(df: DataFrame, nombre: str, salida: str) -> None:
    destino = Path(salida).resolve()
    destino.mkdir(parents=True, exist_ok=True)

    # El resultado es pequenio: se guarda en cache para no recalcularlo en las dos escrituras.
    df = df.cache()

    # Parquet (formato columnar), resultado completo sin redondear.
    df.write.mode("overwrite").parquet((destino / nombre).as_posix())

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
    parser.add_argument("--memoria", default="8g", help="Memoria del driver (ej. 8g)")
    parser.add_argument("--sin-cache", action="store_true", help="No cachear la tabla base")
    args = parser.parse_args()

    inicio = time.perf_counter()
    spark = crear_spark(args.memoria)

    # Hasta aqui todo es perezoso: Spark solo arma el DAG.
    viajes = transformar(limpiar(leer_viajes(spark, args.input)))
    base = unir_zonas(viajes, leer_zonas(spark, args.zones))

    # La tabla base se reutiliza en las 3 agregaciones; con cache se lee y limpia una sola vez.
    if not args.sin_cache:
        base = base.cache()

    print("Calculando y escribiendo resultados...")
    for nombre, df in agregar(base).items():
        escribir(df, nombre, args.output)  # cada escritura es una accion: aqui se ejecuta el DAG

    if not args.sin_cache:
        base.unpersist()

    print(f"Tiempo total: {time.perf_counter() - inicio:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
