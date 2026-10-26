"""Q1 — Quelles stations présentent le plus fort taux d'utilisation ?

Usage = rotation des vélos : somme des mouvements (|Δ vélos| entre snapshots) par
station, ramenée par heure observée pour comparer des stations suivies différemment.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import save_fig, with_movements


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q1] Stations au plus fort taux d'utilisation")
    mv = with_movements(silver)

    usage = (mv.groupBy("network_id", "city", "station_id", "station_name")
             .agg(F.sum("moved").alias("mouvements_total"),
                  F.count("moved").alias("intervalles"),
                  F.avg("occupancy_rate").alias("occupation_moyenne"))
             # mouvements par intervalle observé = intensité d'usage comparable
             .withColumn("usage_par_intervalle",
                         F.col("mouvements_total") / F.col("intervalles"))
             .filter(F.col("intervalles") > 0)
             .orderBy(F.desc("mouvements_total")))

    usage.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q1_utilisation")

    top = usage.limit(config.TOP_N).toPandas()
    print(f"  Top {len(top)} stations (mouvements de vélos) :")
    for _, r in top.head(10).iterrows():
        print(f"    {r['city']:<14} {str(r['station_name'])[:30]:<30} "
              f"{int(r['mouvements_total']):>5} mvts")

    if not top.empty:
        fig, ax = plt.subplots(figsize=(11, 6))
        labels = [f"{str(n)[:24]} ({c})" for n, c in zip(top["station_name"], top["city"])]
        ax.barh(labels[::-1], top["mouvements_total"][::-1], color="#2a9d8f")
        ax.set_xlabel("Mouvements de vélos (|Δ| cumulés)")
        ax.set_title("Q1 — Stations les plus utilisées")
        save_fig(fig, "p2_q1_utilisation.png")
    print("  -> Les stations à forte rotation sont les plus sollicitées du réseau.")
