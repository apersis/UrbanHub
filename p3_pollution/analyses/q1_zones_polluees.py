"""Q1 — Quelles zones présentent les niveaux de pollution les plus élevés ?

On normalise chaque mesure par la valeur guide OMS du polluant (indice = value / guide),
puis on moyenne par ville. Les villes au plus fort indice sont les plus polluées,
toutes substances confondues.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q1] Zones les plus polluées")

    # indice normalisé = valeur / valeur guide OMS
    guide = F.create_map(*sum(([F.lit(k), F.lit(v)] for k, v in config.WHO_GUIDELINES.items()), []))
    df = silver.withColumn("indice", F.col("value") / F.element_at(guide, F.col("pollutant")))

    par_ville = (df.filter(F.col("city").isNotNull())
                 .groupBy("city")
                 .agg(F.avg("indice").alias("indice_moyen"),
                      F.countDistinct("sensor_id").alias("capteurs"))
                 .orderBy(F.desc("indice_moyen")))
    par_ville.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q1_zones_polluees")

    top = par_ville.limit(config.TOP_N).toPandas()
    print("  Villes les plus polluées (indice OMS normalisé) :")
    for _, r in top.head(10).iterrows():
        print(f"    {str(r['city']):<20} indice {r['indice_moyen']:.2f}  ({int(r['capteurs'])} capteurs)")

    if not top.empty:
        fig, ax = plt.subplots(figsize=(11, 6))
        ax.barh(top["city"].astype(str)[::-1], top["indice_moyen"][::-1], color="#6a4c93")
        ax.axvline(1.0, ls="--", color="red", alpha=.6, label="valeur guide OMS")
        ax.set_xlabel("Indice moyen (value / guide OMS)")
        ax.set_title("Q1 — Villes les plus polluées")
        ax.legend()
        save_fig(fig, "p3_q1_zones_polluees.png")
    print("  -> Les villes au-dessus de 1 dépassent en moyenne les valeurs guides OMS.")
