"""Configuration de la Partie 3 (pollution). Connexion : voir `commun`."""
from __future__ import annotations

from commun.config import FIGURES_DIR, OUTPUT_DIR  # noqa: F401 (ré-export)

BRONZE_PATH = "s3a://bronze/pollution/openaq/"       # snapshots JSONL
SILVER_PATH = "s3a://silver/pollution/measurements"  # Parquet typé
GOLD_PREFIX = "s3a://gold/pollution"                 # indicateurs métier

POLLUTANTS = ["pm25", "pm10", "no2", "o3", "co"]

# Valeurs guides OMS 2021 (µg/m³ ; CO en mg/m³) pour normaliser et repérer le
# polluant dominant (= celui qui dépasse le plus sa valeur guide).
WHO_GUIDELINES = {"pm25": 15.0, "pm10": 45.0, "no2": 25.0, "o3": 100.0, "co": 4.0}

ANOMALY_Z = 2.0   # seuil de détection d'épisode anormal (score z)
TOP_N = 15
