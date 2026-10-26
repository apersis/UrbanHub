"""Q4 — Existe-t-il des déséquilibres géographiques dans la disponibilité des vélos ?

On compare les villes sur l'occupation moyenne et les parts de temps à vide / pleine.
Un réseau équilibré oscille autour de 50 % d'occupation ; les écarts marqués révèlent
des déséquilibres géographiques.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import save_fig


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q4] Déséquilibres géographiques")

    par_ville = (silver.groupBy("city")
                 .agg(F.avg("occupancy_rate").alias("occupation_moyenne"),
                      F.avg("is_empty").alias("pct_vide"),
                      F.avg("is_full").alias("pct_pleine"),
                      F.countDistinct("station_id").alias("stations"))
                 .orderBy(F.desc("occupation_moyenne")))
    par_ville.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q4_desequilibres_geo")
    pv = par_ville.toPandas()

    print("  Occupation moyenne par ville :")
    for _, r in pv.head(10).iterrows():
        print(f"    {str(r['city']):<16} occ {100*r['occupation_moyenne']:4.0f} % | "
              f"vide {100*r['pct_vide']:4.0f} % | pleine {100*r['pct_pleine']:4.0f} %")

    if not pv.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        x = np.arange(len(pv))
        ax.bar(x, pv["occupation_moyenne"] * 100, color="#457b9d")
        ax.axhline(50, ls="--", color="black", alpha=.6, label="équilibre (50 %)")
        ax.set_xticks(x)
        ax.set_xticklabels(pv["city"].astype(str), rotation=45, ha="right")
        ax.set_ylabel("Occupation moyenne (%)")
        ax.set_title("Q4 — Déséquilibres de disponibilité entre villes")
        ax.legend()
        save_fig(fig, "p2_q4_desequilibres_geo.png")
    print("  -> Les villes loin de 50 % (très vides ou très pleines) sont déséquilibrées.")
