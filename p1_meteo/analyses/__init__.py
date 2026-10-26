"""Analyses métier de la Partie 1 (couche Gold).

Un module par question métier :
  - q1_anomalies    : périodes météorologiques anormales
  - q2_visibilite   : corrélation météo / visibilité
  - q3_saisonnalite : évolution saisonnière de la température (France & villes)
  - q4_extremes     : jours à conditions extrêmes

Chaque module expose `run(spark, silver_df)` : calcule les indicateurs, les écrit
dans la couche Gold, enregistre une figure PNG dans `output/figures/` et imprime
une interprétation.
"""
from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")  # backend non interactif : on enregistre des PNG
import matplotlib.pyplot as plt  # noqa: E402

from .. import config  # noqa: E402

plt.rcParams["axes.grid"] = True
MOIS = ["Jan", "Fév", "Mar", "Avr", "Mai", "Jun", "Jul", "Aoû", "Sep", "Oct", "Nov", "Déc"]


def ensure_output_dir() -> None:
    os.makedirs(config.FIGURES_DIR, exist_ok=True)


def save_fig(fig, filename: str) -> str:
    """Enregistre une figure dans output/figures/ et renvoie son chemin."""
    ensure_output_dir()
    path = os.path.join(config.FIGURES_DIR, filename)
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  figure -> {path}")
    return path
