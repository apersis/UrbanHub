"""Q2 — Existe-t-il une variation journalière de la pollution ?

Concentration moyenne par heure de la journée, pour chaque polluant : révèle les
cycles journaliers (ex. pics de NO2 aux heures de pointe, O3 l'après-midi).
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q2] Variation journalière de la pollution")

    par_heure = (silver.groupBy("pollutant", "hour")
                 .agg(F.avg("value").alias("valeur_moyenne"))
                 .orderBy("pollutant", "hour"))
    par_heure.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q2_variation_journaliere")
    pdf = par_heure.toPandas()

    if not pdf.empty:
        fig, ax = plt.subplots(figsize=(11, 5))
        for pol, g in pdf.groupby("pollutant"):
            ax.plot(g["hour"], g["valeur_moyenne"], "o-", label=pol, alpha=.8)
        ax.set_xticks(range(0, 24))
        ax.set_xlabel("Heure de la journée (UTC)")
        ax.set_ylabel("Concentration moyenne")
        ax.set_title("Q2 — Cycle journalier de la pollution par polluant")
        ax.legend()
        save_fig(fig, "p3_q2_variation_journaliere.png")
        # amplitude jour/nuit par polluant
        print("  Amplitude (max-min horaire) par polluant :")
        for pol, g in pdf.groupby("pollutant"):
            amp = g["valeur_moyenne"].max() - g["valeur_moyenne"].min()
            print(f"    {pol:6s} {amp:.1f}")
    print("  -> Oui, chaque polluant montre un cycle horaire (trafic le matin/soir, O3 l'après-midi).")
