"""Bronze -> Silver (pollution) : lecture des snapshots JSONL, typage et nettoyage."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config


def lire_bronze(spark: SparkSession) -> DataFrame:
    raw = spark.read.option("recursiveFileLookup", True).json(config.BRONZE_PATH)
    print(f"Bronze : {raw.count():,} mesures lues depuis {config.BRONZE_PATH}")
    return raw


def construire_silver(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.col("sensor_id").cast("string"),
        F.col("pollutant").cast("string"),
        F.col("unit").cast("string"),
        F.col("value").cast("double"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("location_name").cast("string"),
        F.col("city").cast("string"),
        F.to_timestamp("timestamp").alias("ts"),
        F.to_timestamp("ingestion_timestamp").alias("ingested_at"),
    )

    df = df.filter(
        F.col("pollutant").isin(config.POLLUTANTS) &
        F.col("value").isNotNull() & (F.col("value") >= 0) &
        F.col("ts").isNotNull()
    )

    df = (df
          .withColumn("date", F.to_date("ts"))
          .withColumn("hour", F.hour("ts")))

    return df.dropDuplicates(["sensor_id", "pollutant", "ts"])


def ecrire_silver(df: DataFrame) -> None:
    df.write.mode("overwrite").partitionBy("date").parquet(config.SILVER_PATH)
    print(f"Silver écrit dans {config.SILVER_PATH}")


def rapport_qualite(df: DataFrame) -> None:
    total = df.count()
    capteurs = df.select("sensor_id").distinct().count()
    villes = df.select("city").distinct().count()
    tmin = df.agg(F.min("ts")).first()[0]
    tmax = df.agg(F.max("ts")).first()[0]
    print(f"Silver : {total:,} mesures | {capteurs} capteurs | {villes} villes | {tmin} -> {tmax}")
    print("Répartition par polluant :")
    for r in df.groupBy("pollutant").count().orderBy(F.desc("count")).collect():
        print(f"  {r['pollutant']:6s} {r['count']:>8,}")
