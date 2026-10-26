"""Analyses métier de la Partie 3 (couche Gold).

  q1_zones_polluees       : zones (villes) les plus polluées
  q2_variation_journaliere: cycle journalier de la pollution
  q3_polluant_dominant    : polluant dominant par ville
  q4_episodes_anormaux    : épisodes de pollution anormale (score z)

Chaque module expose `run(spark, silver_df)`.
"""
from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from .. import config  # noqa: E402

plt.rcParams["axes.grid"] = True


def save_fig(fig, filename: str) -> str:
    os.makedirs(config.FIGURES_DIR, exist_ok=True)
    path = os.path.join(config.FIGURES_DIR, filename)
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  figure -> {path}")
    return path
