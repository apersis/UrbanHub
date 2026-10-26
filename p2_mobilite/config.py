"""Configuration de la Partie 2 (mobilité). Connexion : voir `commun`."""
from __future__ import annotations

from commun.config import FIGURES_DIR, OUTPUT_DIR  # noqa: F401 (ré-export)

# Mêmes buckets que les autres parties, préfixe "mobility/".
BRONZE_PATH = "s3a://bronze/mobility/citybikes/"     # snapshots JSONL
SILVER_PATH = "s3a://silver/mobility/stations"       # Parquet typé
GOLD_PREFIX = "s3a://gold/mobility"                   # indicateurs métier

# Station "vide" = 0 vélo ; "pleine" = 0 place libre (impossible de rendre un vélo).
CRITICAL_EMPTY_PCT = 0.30   # vide > 30 % du temps  -> offre insuffisante
CRITICAL_FULL_PCT = 0.30    # pleine > 30 % du temps -> à délester
TOP_N = 15
