"""Q3 — Quels polluants sont les plus dominants dans la ville étudiée ?

Pour chaque ville, on classe les polluants par indice normalisé OMS (value / guide) :
le polluant dominant est celui qui dépasse le plus sa valeur guide.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import config
from . import save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q3] Polluant dominant par ville")

    guide = F.create_map(*sum(([F.lit(k), F.lit(v)] for k, v in config.WHO_GUIDELINES.items()), []))
    df = silver.withColumn("indice", F.col("value") / F.element_at(guide, F.col("pollutant")))

    par = (df.filter(F.col("city").isNotNull())
           .groupBy("city", "pollutant")
           .agg(F.avg("indice").alias("indice_moyen"),
                F.avg("value").alias("valeur_moyenne")))

    # polluant dominant = indice max par ville
    w = Window.partitionBy("city").orderBy(F.desc("indice_moyen"))
    dominant = (par.withColumn("rang", F.row_number().over(w))
                .filter(F.col("rang") == 1).drop("rang"))
    dominant.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q3_polluant_dominant")

    pdf = dominant.orderBy(F.desc("indice_moyen")).limit(config.TOP_N).toPandas()
    print("  Polluant dominant (villes les plus concernées) :")
    for _, r in pdf.head(12).iterrows():
        print(f"    {str(r['city']):<20} {r['pollutant']:6s} indice {r['indice_moyen']:.2f}")

    if not pdf.empty:
        counts = pdf["pollutant"].value_counts()
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(counts.index, counts.values, color="#1d7874")
        ax.set_ylabel("Nb de villes où il domine")
        ax.set_title("Q3 — Polluant dominant par ville")
        save_fig(fig, "p3_q3_polluant_dominant.png")
    print("  -> Le polluant dominant varie selon la ville (souvent NO2/PM en milieu urbain).")
