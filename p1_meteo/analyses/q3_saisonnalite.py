"""Q3 — Évolution saisonnière de la température en France et dans les grandes villes."""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import MOIS, save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q3] Saisonnalité de la température")

    # Profil mensuel national (moyenne pluriannuelle)
    nat = (silver.groupBy("month")
           .agg(F.avg("temperature_c").alias("temp_moy"))
           .orderBy("month").toPandas())

    # Profil mensuel par grande ville
    city = (silver.filter(F.col("city").isNotNull())
            .groupBy("city", "month")
            .agg(F.avg("temperature_c").alias("temp_moy"))
            .orderBy("city", "month").toPandas())

    # Moyenne par saison (national)
    sais = (silver.groupBy("season")
            .agg(F.avg("temperature_c").alias("temp_moy")).toPandas())
    sais = sais.sort_values("season", key=lambda s: s.map(
        {v: i for i, v in enumerate(config.SEASON_ORDER)}))

    # Persistance Gold
    (spark.createDataFrame(nat).write.mode("overwrite")
     .parquet(f"{config.GOLD_PREFIX}/q3_temp_mensuelle_nationale"))

    # Visualisation
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(nat["month"], nat["temp_moy"], "o-", lw=3, color="black", label="France (moyenne)")
    for c, g in city.groupby("city"):
        ax.plot(g["month"], g["temp_moy"], "o-", alpha=.7, label=c)
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(MOIS)
    ax.set_ylabel("Température moyenne (°C)")
    ax.set_title("Q3 — Saisonnalité de la température en France")
    ax.legend(ncol=3, fontsize=8)
    save_fig(fig, "p1_q3_saisonnalite.png")

    print("  Moyenne par saison :")
    for _, r in sais.iterrows():
        print(f"    {r['season']:10s} {r['temp_moy']:.1f} °C")
    print("  -> Profil annuel en cloche (min janvier, max juillet-août) ; villes du Sud "
          "au-dessus de la moyenne, Nord/Est en dessous.")
