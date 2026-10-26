"""Q3 — Peut-on identifier des conditions météo favorables à la mobilité douce ?

On croise météo et mobilité par (ville, heure), on classe chaque couple par tranche de
température et présence de pluie, et on regarde dans quelles conditions l'usage des
vélos est le plus fort.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .. import config
from ..profils import weather_hourly, mobility_hourly
from . import save_fig


def executer(spark: SparkSession) -> None:
    print("[Q4-Q3] Conditions favorables à la mobilité douce")
    w = weather_hourly(spark)
    m = mobility_hourly(spark)
    if w is None or m is None:
        print("  Couche météo ou mobilité absente -> question ignorée.")
        return

    joined = (w.join(m, ["city_norm", "hour"], "inner")
              .withColumn("pluie", F.when(F.col("precipitation_mm") > 0.1, "pluie").otherwise("sec"))
              .withColumn("tranche_temp",
                          F.when(F.col("temperature_c") < 5, "<5°C")
                           .when(F.col("temperature_c") < 15, "5-15°C")
                           .when(F.col("temperature_c") < 25, "15-25°C")
                           .otherwise(">25°C")))

    par_cond = (joined.groupBy("tranche_temp", "pluie")
                .agg(F.avg("velos_mouvements").alias("usage_moyen"),
                     F.count("*").alias("n"))
                .orderBy(F.desc("usage_moyen")))
    par_cond.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q3_conditions_favorables")
    pdf = par_cond.toPandas()
    if pdf.empty:
        print("  (pas de données croisées météo/vélos)")
        return

    print("  Usage moyen des vélos par condition :")
    for _, r in pdf.head(8).iterrows():
        print(f"    {r['tranche_temp']:<8} {r['pluie']:<6} usage {r['usage_moyen']:.2f} (n={int(r['n'])})")

    ordre = ["<5°C", "5-15°C", "15-25°C", ">25°C"]
    fig, ax = plt.subplots(figsize=(10, 5))
    for pluie, g in pdf.groupby("pluie"):
        g = g.set_index("tranche_temp").reindex(ordre)
        ax.plot(ordre, g["usage_moyen"], "o-", label=pluie)
    ax.set_xlabel("Tranche de température")
    ax.set_ylabel("Usage moyen des vélos (mouvements)")
    ax.set_title("Q3 — Conditions favorables à la mobilité douce")
    ax.legend()
    save_fig(fig, "p4_q3_conditions_favorables.png")
    print("  -> Mobilité douce maximale par temps doux/chaud et sec ; minimale par froid/pluie.")
