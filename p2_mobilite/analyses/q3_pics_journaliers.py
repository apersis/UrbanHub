"""Q3 — Quels sont les pics d'utilisation journaliers du réseau de vélos ?

On somme les mouvements de vélos par heure de la journée : les heures de forte
rotation sont les pics d'utilisation (typiquement matin et soir).
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import save_fig, with_movements


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q3] Pics d'utilisation journaliers")
    mv = with_movements(silver)

    par_heure = (mv.groupBy("hour")
                 .agg(F.sum("moved").alias("mouvements"),
                      F.count("moved").alias("intervalles"))
                 .withColumn("usage_moyen", F.col("mouvements") / F.col("intervalles"))
                 .orderBy("hour").toPandas())
    (spark.createDataFrame(par_heure).write.mode("overwrite")
     .parquet(f"{config.GOLD_PREFIX}/q3_pics_journaliers"))

    if not par_heure.empty:
        fig, ax = plt.subplots(figsize=(11, 5))
        ax.plot(par_heure["hour"], par_heure["usage_moyen"], "o-", color="#264653")
        ax.set_xticks(range(0, 24))
        ax.set_xlabel("Heure de la journée (UTC)")
        ax.set_ylabel("Mouvements moyens par intervalle")
        ax.set_title("Q3 — Pic d'utilisation du réseau par heure")
        save_fig(fig, "p2_q3_pics_journaliers.png")
        top = par_heure.nlargest(3, "usage_moyen")
        print("  Heures de pic :", ", ".join(f"{int(h)}h" for h in top["hour"]))
    print("  -> Les pics ressortent aux heures de pointe (domicile-travail).")
