"""Analyses métier de la Partie 2 (couche Gold).

Un module par question :
  q1_utilisation        : stations au plus fort taux d'utilisation (rotation)
  q2_offre_insuffisante : stations/zones souvent vides
  q3_pics_journaliers   : pics d'utilisation par heure
  q4_desequilibres_geo  : déséquilibres entre villes
  q5_stations_critiques : stations à rééquilibrer (vides ou pleines)

Chaque module expose `run(spark, silver_df)`.
"""
from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from .. import config  # noqa: E402

plt.rcParams["axes.grid"] = True


def save_fig(fig, filename: str) -> str:
    os.makedirs(config.FIGURES_DIR, exist_ok=True)
    path = os.path.join(config.FIGURES_DIR, filename)
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  figure -> {path}")
    return path


def with_movements(silver: DataFrame) -> DataFrame:
    """Ajoute `moved` = |Δ vélos| entre deux snapshots consécutifs d'une station.

    C'est le proxy d'usage : un vélo pris ou rendu = une unité de mouvement.
    """
    w = Window.partitionBy("network_id", "station_id").orderBy("ts")
    prev = F.lag("bikes_available").over(w)
    return (silver
            .withColumn("prev_bikes", prev)
            .withColumn("moved",
                        F.when(prev.isNull(), None)
                         .otherwise(F.abs(F.col("bikes_available") - prev))))
