"""Orchestrateur Partie 2 (mobilité) : Bronze -> Silver -> Gold.

Exemples :
    python -m p2_mobilite.pipeline
    python -m p2_mobilite.pipeline --step analyses --question q3
"""
from __future__ import annotations

import argparse

from commun import config as commun_cfg
from commun.spark import build_spark

from . import config, nettoyage
from .analyses import (q1_utilisation, q2_offre_insuffisante, q3_pics_journaliers,
                       q4_desequilibres_geo, q5_stations_critiques)

QUESTIONS = {
    "q1": q1_utilisation,
    "q2": q2_offre_insuffisante,
    "q3": q3_pics_journaliers,
    "q4": q4_desequilibres_geo,
    "q5": q5_stations_critiques,
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Pipeline Partie 2 UrbanHub - mobilité.")
    p.add_argument("--step", choices=["silver", "analyses", "all"], default="all")
    p.add_argument("--question", choices=list(QUESTIONS), default=None)
    args = p.parse_args(argv)

    spark = build_spark("UrbanHub - Partie 2 Mobilite")

    if args.step in ("silver", "all"):
        raw = nettoyage.lire_bronze(spark)
        nettoyage.ecrire_silver(nettoyage.construire_silver(raw))

    if args.step in ("analyses", "all"):
        silver = spark.read.parquet(config.SILVER_PATH).cache()
        nettoyage.rapport_qualite(silver)
        questions = {args.question: QUESTIONS[args.question]} if args.question else QUESTIONS
        for name, module in questions.items():
            print(f"\n===== {name.upper()} =====")
            module.executer(spark, silver)

    print("\nPipeline Partie 2 terminé.")
    if not commun_cfg.SPARK_REMOTE:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
