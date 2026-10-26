"""Q5 — Peut-on détecter des stations critiques nécessitant un rééquilibrage ?

Une station est critique si elle est trop souvent VIDE (pas de vélo à prendre) ou
trop souvent PLEINE (pas de place pour rendre). On les détecte via les seuils de
config et on les classe pour prioriser le rééquilibrage.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q5] Stations critiques à rééquilibrer")

    stats = (silver.groupBy("city", "station_id", "station_name", "latitude", "longitude")
             .agg(F.avg("is_empty").alias("pct_vide"),
                  F.avg("is_full").alias("pct_pleine"),
                  F.count("*").alias("releves")))

    critiques = (stats
                 .withColumn("critique_vide", F.col("pct_vide") >= config.CRITICAL_EMPTY_PCT)
                 .withColumn("critique_pleine", F.col("pct_pleine") >= config.CRITICAL_FULL_PCT)
                 .withColumn("critique", F.col("critique_vide") | F.col("critique_pleine"))
                 # score = temps passé en situation bloquante (vide ou pleine)
                 .withColumn("score_critique", F.greatest("pct_vide", "pct_pleine")))
    critiques.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q5_stations_critiques")

    total = stats.count()
    nb_crit = critiques.filter("critique").count()
    print(f"  {nb_crit} / {total} stations critiques "
          f"(vide ou pleine >= {int(config.CRITICAL_EMPTY_PCT*100)} % du temps)")

    top = (critiques.filter("critique").orderBy(F.desc("score_critique"))
           .limit(config.TOP_N).toPandas())
    for _, r in top.head(10).iterrows():
        etat = "VIDE" if r["pct_vide"] >= r["pct_pleine"] else "PLEINE"
        print(f"    {str(r['city']):<14} {str(r['station_name'])[:28]:<28} "
              f"{etat} {100*r['score_critique']:3.0f} % du temps")

    if not top.empty:
        fig, ax = plt.subplots(figsize=(11, 6))
        labels = [f"{str(n)[:24]} ({c})" for n, c in zip(top["station_name"], top["city"])]
        ax.barh(labels[::-1], top["score_critique"][::-1] * 100, color="#bc4749")
        ax.set_xlabel("% du temps en situation bloquante (vide ou pleine)")
        ax.set_title("Q5 — Stations critiques à rééquilibrer")
        save_fig(fig, "p2_q5_stations_critiques.png")
    print("  -> Ces stations (souvent vides ou pleines) sont à rééquilibrer en priorité.")
