"""Q4 — Quels sont les jours présentant des conditions météorologiques extrêmes ?

Agrégation journalière nationale (toutes stations) : T max/min, vent max, pluie max.
Repère les jours dépassant les seuils d'alerte définis dans config.EXTREME_THRESHOLDS.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q4] Jours à conditions extrêmes")
    th = config.EXTREME_THRESHOLDS

    daily = silver.groupBy("date").agg(
        F.max("temperature_c").alias("tmax"),
        F.min("temperature_c").alias("tmin"),
        F.max("wind_speed_ms").alias("vent_max"),
        F.max("precipitation_mm").alias("pluie_max"),
    )

    extremes = (daily
                .withColumn("canicule", F.col("tmax") >= th["canicule_tmax"])
                .withColumn("grand_froid", F.col("tmin") <= th["grand_froid_tmin"])
                .withColumn("tempete", F.col("vent_max") >= th["tempete_vent"])
                .withColumn("fortes_pluies", F.col("pluie_max") >= th["fortes_pluies_mm"])
                .withColumn("extreme",
                            F.col("tmax").isNotNull() &
                            (F.col("canicule") | F.col("grand_froid") |
                             F.col("tempete") | F.col("fortes_pluies"))))

    extremes.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q4_jours_extremes")

    pdf = extremes.orderBy("date").toPandas()
    pdf["date"] = pd.to_datetime(pdf["date"])
    nb = {k: int(pdf[k].sum()) for k in ["canicule", "grand_froid", "tempete", "fortes_pluies"]}
    print(f"  Jours par type d'extrême : {nb}")

    # Visualisation : séries T max / T min avec seuils et jours extrêmes surlignés
    fig, ax = plt.subplots(figsize=(13, 5))
    ax.plot(pdf["date"], pdf["tmax"], color="#d1495b", lw=.7, label="T max (°C)")
    ax.plot(pdf["date"], pdf["tmin"], color="#00798c", lw=.7, label="T min (°C)")
    ax.axhline(th["canicule_tmax"], ls="--", color="red", alpha=.6)
    ax.axhline(th["grand_froid_tmin"], ls="--", color="blue", alpha=.6)
    hot = pdf[pdf["canicule"]]
    cold = pdf[pdf["grand_froid"]]
    ax.scatter(hot["date"], hot["tmax"], color="red", s=20, zorder=5, label="Canicule")
    ax.scatter(cold["date"], cold["tmin"], color="blue", s=20, zorder=5, label="Grand froid")
    ax.set_ylabel("Température (°C)")
    ax.set_title("Q4 — Jours extrêmes (températures)")
    ax.legend(ncol=4, fontsize=8)
    save_fig(fig, "p1_q4_extremes.png")

    print("  Top 5 jours les plus chauds :")
    for _, r in pdf.nlargest(5, "tmax").iterrows():
        print(f"    {r['date'].date()}  Tmax {r['tmax']:.1f}°C")
    print("  Top 5 jours les plus venteux :")
    for _, r in pdf.nlargest(5, "vent_max").iterrows():
        print(f"    {r['date'].date()}  vent {r['vent_max']:.1f} m/s")
    print("  -> Les épisodes extrêmes sont horodatés et réutilisables pour le croisement "
          "météo × pollution × mobilité (Partie 4).")
