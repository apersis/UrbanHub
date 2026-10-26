"""Q2 — Existe-t-il une corrélation entre conditions météorologiques et visibilité ?

Calcule les corrélations de Pearson de la visibilité avec les autres variables
(dont l'écart température / point de rosée, proxy d'humidité), trace la matrice de
corrélation et des nuages de points.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from . import save_fig

_CORR_VARS = ["temperature_c", "dewpoint_c", "wind_speed_ms",
              "pressure_hpa", "precipitation_mm", "spread_t_td"]


def executer(spark: SparkSession, silver: DataFrame) -> None:
    print("[Q2] Corrélation météo / visibilité")

    cols = ["visibility_m", "temperature_c", "dewpoint_c",
            "wind_speed_ms", "pressure_hpa", "precipitation_mm"]
    sdf = (silver.select(*cols)
           .withColumn("spread_t_td", F.col("temperature_c") - F.col("dewpoint_c"))
           .dropna())

    # Corrélations visibilité <-> chaque variable (calcul distribué, sans tout rapatrier)
    corr_vis = {c: sdf.stat.corr("visibility_m", c) for c in _CORR_VARS}
    corr_rows = [(k, float(v)) for k, v in corr_vis.items()]
    (spark.createDataFrame(corr_rows, ["variable", "correlation_avec_visibilite"])
     .write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q2_correlations_visibilite"))

    print("  Corrélation de la visibilité avec :")
    for k, v in sorted(corr_vis.items(), key=lambda kv: -abs(kv[1])):
        print(f"    {k:16s} r = {v:+.3f}")

    # Échantillon pour la matrice et les nuages de points
    n = sdf.count()
    frac = min(1.0, 200000 / n) if n else 1.0
    sample = sdf.sample(fraction=frac, seed=42).toPandas()

    # Matrice de corrélation
    M = sample.corr()
    fig, ax = plt.subplots(figsize=(7, 6))
    im = ax.imshow(M, vmin=-1, vmax=1, cmap="coolwarm")
    ax.set_xticks(range(len(M)))
    ax.set_xticklabels(M.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(M)))
    ax.set_yticklabels(M.columns)
    for i in range(len(M)):
        for j in range(len(M)):
            ax.text(j, i, f"{M.iloc[i, j]:.2f}", ha="center", va="center", fontsize=8)
    fig.colorbar(im, fraction=.046)
    ax.set_title("Q2 — Matrice de corrélation")
    save_fig(fig, "p1_q2_matrice_correlation.png")

    # Nuages de points : visibilité vs humidité (T-Td) et vs vent
    s2 = sample.sample(min(len(sample), 8000), random_state=0)
    fig, axs = plt.subplots(1, 2, figsize=(13, 5))
    axs[0].scatter(s2["spread_t_td"], s2["visibility_m"], s=4, alpha=.2)
    axs[0].set_xlabel("Écart T - Td (°C) — humidité")
    axs[0].set_ylabel("Visibilité (m)")
    axs[0].set_title("Visibilité vs humidité")
    axs[1].scatter(s2["wind_speed_ms"], s2["visibility_m"], s=4, alpha=.2, color="#00798c")
    axs[1].set_xlabel("Vitesse du vent (m/s)")
    axs[1].set_ylabel("Visibilité (m)")
    axs[1].set_title("Visibilité vs vent")
    save_fig(fig, "p1_q2_nuages_visibilite.png")

    print("  -> La visibilité dépend surtout de l'humidité (écart T-Td faible => brouillard) "
          "et des précipitations (corrélation négative) ; le vent joue un rôle secondaire.")
