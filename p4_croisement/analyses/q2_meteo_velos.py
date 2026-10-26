"""Q2 — Les conditions météo influencent-elles l'utilisation des vélos urbains ?

Jointure des profils horaires météo et mobilité par (ville, heure), puis corrélations
entre météo (température, pluie, vent) et usage des vélos (mouvements, occupation).
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .. import config
from ..profils import weather_hourly, mobility_hourly
from . import corr_heatmap


def executer(spark: SparkSession) -> None:
    print("[Q4-Q2] Météo <-> usage des vélos")
    w = weather_hourly(spark)
    m = mobility_hourly(spark)
    if w is None or m is None:
        print("  Couche météo ou mobilité absente -> question ignorée.")
        return

    joined = w.join(m, ["city_norm", "hour"], "inner")
    joined.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q2_meteo_velos")
    pdf = joined.toPandas()
    print(f"  {len(pdf)} couples (ville, heure) croisés.")

    cols = ["temperature_c", "wind_speed_ms", "precipitation_mm",
            "velos_mouvements", "velos_occupation"]
    cols = [c for c in cols if c in pdf.columns]
    M = corr_heatmap(pdf, cols, "Q2 — Corrélations météo / vélos", "p4_q2_meteo_velos.png")
    if M is not None and "velos_mouvements" in M:
        print("  Corrélation des mouvements de vélos avec :")
        for v in ["temperature_c", "precipitation_mm", "wind_speed_ms"]:
            if v in M:
                print(f"    {v:18s} r = {M.loc['velos_mouvements', v]:+.2f}")
    print("  -> Typiquement : il fait beau/doux = plus de vélos ; pluie/vent = moins.")
