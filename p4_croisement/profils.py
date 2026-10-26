"""Profils horaires (ville x heure) par domaine, pour l'analyse croisée.

Chaque loader lit la couche Silver correspondante et renvoie un DataFrame agrégé par
(city_norm, hour), ou None si la couche n'existe pas encore (partie non collectée).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from p1_meteo import config as meteo_cfg
from p2_mobilite import config as mobilite_cfg
from p2_mobilite.analyses import with_movements
from p3_pollution import config as pollution_cfg

# Normalisation des noms de ville : majuscules, sans accents, espaces rognés.
_ACC_SRC = "ÀÂÄÁÃÇÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÑ"
_ACC_DST = "AAAAACEEEEIIIIOOOOOUUUUN"


def city_norm(col: str = "city") -> Column:
    return F.translate(F.upper(F.trim(F.col(col))), _ACC_SRC, _ACC_DST)


def _safe_read(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        return spark.read.parquet(path)
    except Exception:  # noqa: BLE001  (couche absente)
        return None


def weather_hourly(spark: SparkSession) -> DataFrame | None:
    df = _safe_read(spark, meteo_cfg.SILVER_PATH)
    if df is None:
        return None
    return (df.filter(F.col("city").isNotNull())
            .withColumn("city_norm", city_norm())
            .groupBy("city_norm", "hour")
            .agg(F.avg("temperature_c").alias("temperature_c"),
                 F.avg("wind_speed_ms").alias("wind_speed_ms"),
                 F.avg("precipitation_mm").alias("precipitation_mm"),
                 F.avg("visibility_m").alias("visibility_m")))


def mobility_hourly(spark: SparkSession) -> DataFrame | None:
    df = _safe_read(spark, mobilite_cfg.SILVER_PATH)
    if df is None:
        return None
    mv = with_movements(df).withColumn("city_norm", city_norm())
    return (mv.groupBy("city_norm", "hour")
            .agg(F.avg("moved").alias("velos_mouvements"),
                 F.avg("occupancy_rate").alias("velos_occupation"),
                 F.avg("is_empty").alias("velos_pct_vide")))


def pollution_hourly(spark: SparkSession) -> DataFrame | None:
    df = _safe_read(spark, pollution_cfg.SILVER_PATH)
    if df is None:
        return None
    guide = F.create_map(*sum(([F.lit(k), F.lit(v)]
                               for k, v in pollution_cfg.WHO_GUIDELINES.items()), []))
    df = df.withColumn("city_norm", city_norm()).withColumn("indice", F.col("value") / F.element_at(guide, F.col("pollutant")))
    indice = df.groupBy("city_norm", "hour").agg(F.avg("indice").alias("pollution_indice"))
    pivot = (df.groupBy("city_norm", "hour").pivot("pollutant", pollution_cfg.POLLUTANTS).avg("value"))
    return indice.join(pivot, ["city_norm", "hour"], "left")
