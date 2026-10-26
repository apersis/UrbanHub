"""Q4 — Existe-t-il des périodes où mobilité, pollution et météo interagissent fortement ?

Jointure des trois profils par (ville, heure). On normalise (min-max) l'intensité de
chaque domaine — pollution (indice), mobilité (mouvements), météo (|température - moyenne|
+ vent) — et on somme en un "indice d'interaction". Les (ville, heure) au plus fort indice
sont les moments où les trois flux sont simultanément intenses.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

from .. import config
from ..profils import weather_hourly, mobility_hourly, pollution_hourly
from . import save_fig


def _minmax(s):
    rng = s.max() - s.min()
    return (s - s.min()) / rng if rng else s * 0.0


def executer(spark: SparkSession) -> None:
    print("[Q4-Q4] Interactions météo x mobilité x pollution")
    w, m, p = weather_hourly(spark), mobility_hourly(spark), pollution_hourly(spark)
    if w is None or m is None or p is None:
        print("  Les trois couches (météo, mobilité, pollution) sont nécessaires -> ignorée.")
        return

    joined = (w.join(m, ["city_norm", "hour"], "inner")
              .join(p, ["city_norm", "hour"], "inner"))
    joined.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q4_interactions")
    pdf = joined.toPandas()
    print(f"  {len(pdf)} couples (ville, heure) présents dans les trois flux.")
    if len(pdf) < 3:
        print("  (pas assez de recouvrement entre les trois flux pour un indice fiable)")
        return

    # intensité météo = écart de température à la moyenne + vent (normalisés)
    pdf["meteo_intensite"] = (pdf["temperature_c"] - pdf["temperature_c"].mean()).abs()
    pdf["i_pollution"] = _minmax(pdf["pollution_indice"])
    pdf["i_mobilite"] = _minmax(pdf["velos_mouvements"])
    pdf["i_meteo"] = _minmax(pdf["meteo_intensite"] + _minmax(pdf["wind_speed_ms"]))
    pdf["indice_interaction"] = pdf[["i_pollution", "i_mobilite", "i_meteo"]].mean(axis=1)

    top = pdf.sort_values("indice_interaction", ascending=False).head(15)
    print("  Top (ville, heure) d'interaction forte :")
    for _, r in top.head(10).iterrows():
        print(f"    {str(r['city_norm']):<16} {int(r['hour']):>2}h  "
              f"interaction {r['indice_interaction']:.2f} "
              f"(poll {r['i_pollution']:.2f} / vélos {r['i_mobilite']:.2f} / météo {r['i_meteo']:.2f})")

    labels = [f"{c} {int(h)}h" for c, h in zip(top["city_norm"], top["hour"])]
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.barh(labels[::-1], top["indice_interaction"][::-1], color="#5f0f40")
    ax.set_xlabel("Indice d'interaction (moyenne normalisée des 3 flux)")
    ax.set_title("Q4 — Périodes de forte interaction météo x mobilité x pollution")
    save_fig(fig, "p4_q4_interactions.png")
    print("  -> Ces créneaux combinent forte pollution, fort usage vélo et météo marquée.")
