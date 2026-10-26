"""Q4 — Peut-on identifier des épisodes de pollution anormale ?

Pour chaque capteur/polluant, on calcule un score z (value - moyenne) / écart-type
sur sa série. Les mesures |z| >= seuil sont des épisodes anormaux (pics de pollution).
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import config
from . import save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q4] Épisodes de pollution anormale")

    w = Window.partitionBy("sensor_id", "pollutant")
    df = (silver
          .withColumn("moy", F.avg("value").over(w))
          .withColumn("ect", F.stddev("value").over(w))
          .withColumn("zscore",
                      F.when(F.col("ect") > 0, (F.col("value") - F.col("moy")) / F.col("ect")))
          .withColumn("anormal", F.col("zscore") >= config.ANOMALY_Z))

    anomalies = df.filter("anormal")
    anomalies.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q4_episodes_anormaux")

    nb = anomalies.count()
    total = df.filter(F.col("zscore").isNotNull()).count()
    print(f"  {nb:,} mesures anormales (z >= {config.ANOMALY_Z}) sur {total:,}")

    top = (anomalies.orderBy(F.desc("zscore")).limit(config.TOP_N)
           .select("city", "pollutant", "value", "zscore", "ts").toPandas())
    for _, r in top.head(10).iterrows():
        print(f"    {str(r['city']):<16} {r['pollutant']:6s} {r['value']:7.1f} "
              f"(z={r['zscore']:.1f}) {r['ts']}")

    # nb d'épisodes par polluant
    par_pol = (anomalies.groupBy("pollutant").count().orderBy(F.desc("count")).toPandas())
    if not par_pol.empty:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(par_pol["pollutant"], par_pol["count"], color="#9e2a2b")
        ax.set_ylabel("Nb d'épisodes anormaux")
        ax.set_title(f"Q4 — Épisodes de pollution anormale (z >= {config.ANOMALY_Z})")
        save_fig(fig, "p3_q4_episodes_anormaux.png")
    print("  -> Les pics détectés (z élevé) correspondent à des épisodes de pollution.")
