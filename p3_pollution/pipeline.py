"""Orchestrateur Partie 3 (pollution) : Bronze -> Silver -> Gold.

Exemples :
    python -m p3_pollution.pipeline
    python -m p3_pollution.pipeline --step analyses --question q2
"""
from __future__ import annotations

import argparse

from commun import config as commun_cfg
from commun.spark import build_spark

from . import config, nettoyage
from .analyses import (q1_zones_polluees, q2_variation_journaliere,
                       q3_polluant_dominant, q4_episodes_anormaux)

QUESTIONS = {
    "q1": q1_zones_polluees,
    "q2": q2_variation_journaliere,
    "q3": q3_polluant_dominant,
    "q4": q4_episodes_anormaux,
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Pipeline Partie 3 UrbanHub - pollution.")
    p.add_argument("--step", choices=["silver", "analyses", "all"], default="all")
    p.add_argument("--question", choices=list(QUESTIONS), default=None)
    args = p.parse_args(argv)

    spark = build_spark("UrbanHub - Partie 3 Pollution")

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

    print("\nPipeline Partie 3 terminé.")
    if not commun_cfg.SPARK_REMOTE:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
