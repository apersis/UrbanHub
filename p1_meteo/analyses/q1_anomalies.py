"""Q1 — Peut-on identifier des périodes météorologiques anormales ?

Méthode : comparer la température moyenne de chaque mois (année × mois) à la normale
climatologique du mois (moyenne pluriannuelle), puis calculer un score z
(anomalie / écart-type). Un mois est jugé anormal si |z| >= 2.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import config
from . import save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q1] Périodes anormales (anomalies de température)")

    # Moyenne par (année, mois)
    mm = silver.groupBy("year", "month").agg(F.avg("temperature_c").alias("temp_moy"))

    # Normale climatologique par mois (moyenne + écart-type sur toutes les années)
    w = Window.partitionBy("month")
    mm = (mm
          .withColumn("normale", F.avg("temp_moy").over(w))
          .withColumn("ecart_type", F.stddev("temp_moy").over(w))
          .withColumn("anomalie", F.col("temp_moy") - F.col("normale"))
          .withColumn("zscore", F.col("anomalie") / F.col("ecart_type"))
          .withColumn("anormal", F.abs(F.col("zscore")) >= 2))

    mm.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q1_anomalies_mensuelles")

    pdf = mm.orderBy("year", "month").toPandas()
    pdf["periode"] = pdf["year"].astype(str) + "-" + pdf["month"].astype(str).str.zfill(2)
    pdf["dt"] = pd.to_datetime(pdf["periode"] + "-01")

    # Visualisation : barres d'anomalie + mois anormaux surlignés
    fig, ax = plt.subplots(figsize=(13, 5))
    colors = np.where(pdf["anomalie"] >= 0, "#d1495b", "#00798c")
    ax.bar(pdf["dt"], pdf["anomalie"], width=20, color=colors)
    ax.axhline(0, color="black", lw=.8)
    anom = pdf[pdf["anormal"]]
    ax.scatter(anom["dt"], anom["anomalie"], color="gold", edgecolor="black",
               zorder=5, s=70, label=f"Mois anormaux |z|>=2 ({len(anom)})")
    ax.set_ylabel("Anomalie de température (°C)")
    ax.set_title("Q1 — Anomalies mensuelles vs normale climatologique")
    ax.legend()
    save_fig(fig, "p1_q1_anomalies.png")

    top = anom.reindex(anom["zscore"].abs().sort_values(ascending=False).index)
    print("  Périodes les plus anormales :")
    for _, r in top.head(8).iterrows():
        print(f"    {r['periode']}  {r['temp_moy']:5.1f}°C (normale {r['normale']:4.1f}) "
              f"anomalie {r['anomalie']:+.1f}  z={r['zscore']:+.1f}")
    print("  -> Oui : la méthode du score z isole automatiquement les mois hors-normes "
          "(étés caniculaires, vagues de froid).")
