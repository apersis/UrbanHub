"""Configuration partagée : connexion Spark Connect et Data Lake MinIO."""
from __future__ import annotations

import os

# Spark Connect : Spark tourne dans Docker, le client Python local s'y connecte.
# Mettre SPARK_REMOTE="" pour une session Spark classique (locale/en conteneur).
SPARK_REMOTE = os.getenv("SPARK_REMOTE", "sc://localhost:15002")

# MinIO (utilisé par build_spark en mode classique ; les collecteurs lisent ces
# mêmes variables d'environnement directement).
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))

# Dossier des figures PNG produites par les analyses (toutes parties).
OUTPUT_DIR = os.getenv("URBANHUB_OUTPUT", "output")
FIGURES_DIR = os.path.join(OUTPUT_DIR, "figures")
