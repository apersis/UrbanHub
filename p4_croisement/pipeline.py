"""Orchestrateur Partie 4 : analyse croisée (lit les Silver P1/P2/P3).

Exemples :
    python -m p4_croisement.pipeline
    python -m p4_croisement.pipeline --question q2
"""
from __future__ import annotations

import argparse

from commun import config as commun_cfg
from commun.spark import build_spark

from .analyses import (q1_meteo_pollution, q2_meteo_velos,
                       q3_conditions_favorables, q4_interactions)

QUESTIONS = {
    "q1": q1_meteo_pollution,
    "q2": q2_meteo_velos,
    "q3": q3_conditions_favorables,
    "q4": q4_interactions,
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Pipeline Partie 4 UrbanHub - analyse croisée.")
    p.add_argument("--question", choices=list(QUESTIONS), default=None)
    args = p.parse_args(argv)

    spark = build_spark("UrbanHub - Partie 4 Analyse croisee")

    questions = {args.question: QUESTIONS[args.question]} if args.question else QUESTIONS
    for name, module in questions.items():
        print(f"\n===== {name.upper()} =====")
        module.executer(spark)

    print("\nPipeline Partie 4 terminé.")
    if not commun_cfg.SPARK_REMOTE:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
