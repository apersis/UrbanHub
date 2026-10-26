"""Analyses croisées (Partie 4).

  q1_meteo_pollution     : relation météo <-> pollution
  q2_meteo_velos         : relation météo <-> usage des vélos
  q3_conditions_favorables : conditions météo favorables à la mobilité douce
  q4_interactions        : périodes/villes où les trois flux interagissent fortement

Chaque module expose `run(spark)` (il lit lui-même les couches Silver nécessaires).
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


def corr_heatmap(pdf, cols, title, filename):
    """Trace une matrice de corrélation (pandas) pour les colonnes données."""
    sub = pdf[cols].dropna()
    if len(sub) < 3:
        print(f"  (pas assez de données croisées pour {title} : {len(sub)} lignes)")
        return None
    M = sub.corr()
    fig, ax = plt.subplots(figsize=(1.2 * len(cols) + 3, len(cols) + 2))
    im = ax.imshow(M, vmin=-1, vmax=1, cmap="coolwarm")
    ax.set_xticks(range(len(M))); ax.set_xticklabels(M.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(M))); ax.set_yticklabels(M.columns)
    for i in range(len(M)):
        for j in range(len(M)):
            ax.text(j, i, f"{M.iloc[i, j]:.2f}", ha="center", va="center", fontsize=8)
    fig.colorbar(im, fraction=.046)
    ax.set_title(title)
    save_fig(fig, filename)
    return M
