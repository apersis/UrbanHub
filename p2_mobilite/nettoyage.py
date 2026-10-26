"""Bronze -> Silver (mobilité) : lecture des snapshots JSONL, typage et métriques.

Chaque ligne = l'état d'une station à un instant. On en déduit capacité, taux
d'occupation et drapeaux vide/pleine, utilisés par les analyses.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config


def lire_bronze(spark: SparkSession) -> DataFrame:
    raw = spark.read.option("recursiveFileLookup", True).json(config.BRONZE_PATH)
    print(f"Bronze : {raw.count():,} relevés station x snapshot lus depuis {config.BRONZE_PATH}")
    return raw


def construire_silver(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.col("network_id").cast("string"),
        F.col("network_name").cast("string"),
        F.col("city").cast("string"),
        F.col("station_id").cast("string"),
        F.col("station_name").cast("string"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("bikes_available").cast("int"),
        F.col("free_slots").cast("int"),
        F.to_timestamp("ingestion_timestamp").alias("ts"),
    )

    df = df.filter(
        F.col("station_id").isNotNull() & F.col("ts").isNotNull() &
        (F.col("bikes_available") >= 0) & (F.col("free_slots") >= 0)
    )

    df = (df
          .withColumn("capacity", F.col("bikes_available") + F.col("free_slots"))
          .filter(F.col("capacity") > 0)
          .withColumn("occupancy_rate", F.col("bikes_available") / F.col("capacity"))
          .withColumn("is_empty", (F.col("bikes_available") == 0).cast("int"))
          .withColumn("is_full", (F.col("free_slots") == 0).cast("int"))
          .withColumn("date", F.to_date("ts"))
          .withColumn("hour", F.hour("ts"))
          .withColumn("weekday", F.dayofweek("ts")))

    return df.dropDuplicates(["network_id", "station_id", "ts"])


def ecrire_silver(df: DataFrame) -> None:
    df.write.mode("overwrite").partitionBy("date").parquet(config.SILVER_PATH)
    print(f"Silver écrit dans {config.SILVER_PATH}")


def rapport_qualite(df: DataFrame) -> None:
    total = df.count()
    stations = df.select("network_id", "station_id").distinct().count()
    snaps = df.select("ts").distinct().count()
    villes = df.select("city").distinct().count()
    tmin = df.agg(F.min("ts")).first()[0]
    tmax = df.agg(F.max("ts")).first()[0]
    print(f"Silver : {total:,} relevés | {stations} stations | {villes} villes | "
          f"{snaps} snapshots | {tmin} -> {tmax}")
