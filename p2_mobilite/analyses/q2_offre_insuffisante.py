"""Q2 — Peut-on identifier des zones où l'offre de vélos est insuffisante ?

Une station à court de vélos (0 dispo) ne peut pas servir. On mesure, par station
et par ville, la part du temps passée à vide ; les zones à fort % de vide ont une
offre insuffisante.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q2] Zones où l'offre de vélos est insuffisante")

    # Par station : % du temps à vide
    par_station = (silver.groupBy("city", "station_id", "station_name",
                                  "latitude", "longitude")
                   .agg(F.avg("is_empty").alias("pct_vide"),
                        F.avg("occupancy_rate").alias("occupation_moyenne"),
                        F.count("*").alias("releves"))
                   .orderBy(F.desc("pct_vide")))
    par_station.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q2_offre_insuffisante")

    # Par ville : % moyen de stations/temps à vide
    par_ville = (silver.groupBy("city")
                 .agg(F.avg("is_empty").alias("pct_vide"),
                      F.countDistinct("station_id").alias("stations"))
                 .orderBy(F.desc("pct_vide")).toPandas())

    print("  % de temps à vide par ville :")
    for _, r in par_ville.head(10).iterrows():
        print(f"    {str(r['city']):<16} {100*r['pct_vide']:5.1f} %  ({int(r['stations'])} stations)")

    if not par_ville.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(par_ville["city"].astype(str), par_ville["pct_vide"] * 100, color="#e76f51")
        ax.set_ylabel("% du temps à vide")
        ax.set_title("Q2 — Offre insuffisante : % de temps à vide par ville")
        plt.xticks(rotation=45, ha="right")
        save_fig(fig, "p2_q2_offre_insuffisante.png")
    print("  -> Les villes/stations à fort % de vide manquent de vélos (offre insuffisante).")
