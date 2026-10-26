"""Bronze -> Silver (météo) : lecture des CSV ISD bruts, décodage et nettoyage.

C'est ici que vit le "Travail demandé" de la Partie 1 : extraction des variables
(champs composés ISD, facteurs d'échelle, sentinelles) + nettoyage.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config


def lire_bronze(spark: SparkSession) -> DataFrame:
    """Lit tous les CSV ISD bruts de la couche Bronze (tout en string)."""
    raw = (spark.read
           .option("header", True)
           .option("inferSchema", False)
           .option("recursiveFileLookup", True)
           .csv(config.BRONZE_PATH))
    print(f"Bronze : {raw.count():,} observations brutes lues depuis {config.BRONZE_PATH}")
    return raw


def _sous_champ(colname: str, idx: int) -> Column:
    return F.split(F.col(colname), ",").getItem(idx)


def _decoder(colname: str, idx: int, sentinelle: int, facteur: int) -> Column:
    """cast double, sentinelle -> null, application du facteur d'échelle."""
    v = _sous_champ(colname, idx).cast("double")
    v = F.when(v == sentinelle, None).otherwise(v)
    return (v / facteur) if facteur != 1 else v


def construire_silver(raw: DataFrame) -> DataFrame:
    """Décodage ISD + nettoyage + enrichissement (ville, saison, dimensions temps)."""
    cols = [
        F.col("STATION").alias("station_id"),
        F.to_timestamp("DATE").alias("timestamp"),
        F.col("NAME").alias("station_name"),
        F.col("LATITUDE").cast("double").alias("latitude"),
        F.col("LONGITUDE").cast("double").alias("longitude"),
    ]
    for nom, (champ, idx, sentinelle, facteur) in config.ISD_FIELDS.items():
        cols.append(_decoder(champ, idx, sentinelle, facteur).alias(nom))
    df = raw.select(*cols)

    # Nettoyage : bornes physiques (valeurs aberrantes -> null)
    for col_name, (lo, hi) in config.PHYSICAL_BOUNDS.items():
        df = df.withColumn(col_name, F.when(F.col(col_name).between(lo, hi), F.col(col_name)))
    df = df.filter(F.col("timestamp").isNotNull())

    # Enrichissement temporel + saison météorologique (hémisphère nord)
    df = (df
          .withColumn("year", F.year("timestamp"))
          .withColumn("month", F.month("timestamp"))
          .withColumn("hour", F.hour("timestamp"))
          .withColumn("date", F.to_date("timestamp"))
          .withColumn("season",
                      F.when(F.col("month").isin(12, 1, 2), "Hiver")
                       .when(F.col("month").isin(3, 4, 5), "Printemps")
                       .when(F.col("month").isin(6, 7, 8), "Été")
                       .otherwise("Automne")))

    # Rattachement aux grandes villes (mots-clés du nom de station)
    name = F.upper(F.col("station_name"))
    city_col = F.lit(None).cast("string")
    for city, keywords in config.CITY_KEYWORDS.items():
        cond = F.lit(False)
        for kw in keywords:
            cond = cond | name.contains(kw)
        city_col = F.when(cond, city).otherwise(city_col)
    df = df.withColumn("city", city_col)

    # Déduplication : un relevé par station et par instant
    return df.dropDuplicates(["station_id", "timestamp"])


def ecrire_silver(df: DataFrame) -> None:
    df.write.mode("overwrite").partitionBy("year").parquet(config.SILVER_PATH)
    print(f"Silver écrit dans {config.SILVER_PATH}")


def rapport_qualite(df: DataFrame) -> None:
    total = df.count()
    stations = df.select("station_id").distinct().count()
    dmin = df.agg(F.min("date")).first()[0]
    dmax = df.agg(F.max("date")).first()[0]
    print(f"Silver : {total:,} observations | {stations} stations | {dmin} -> {dmax}")
    print("Complétude (% non-null) :")
    for c in ["temperature_c", "wind_speed_ms", "wind_direction_deg",
              "pressure_hpa", "visibility_m", "precipitation_mm"]:
        nn = df.filter(F.col(c).isNotNull()).count()
        print(f"  {c:20s} {100 * nn / total:5.1f} %")
