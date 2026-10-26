"""Q1 — Existe-t-il une relation entre conditions météorologiques et pollution ?

Jointure des profils horaires météo et pollution par (ville, heure), puis corrélations
(ex. le vent disperse les polluants -> corrélation négative ; l'ensoleillement/chaleur
favorise l'ozone -> corrélation positive).
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .. import config
from ..profils import weather_hourly, pollution_hourly
from . import corr_heatmap


def executer(spark: SparkSession) -> None:
    print("[Q4-Q1] Météo <-> pollution")
    w = weather_hourly(spark)
    p = pollution_hourly(spark)
    if w is None or p is None:
        print("  Couche météo ou pollution absente -> question ignorée.")
        return

    joined = w.join(p, ["city_norm", "hour"], "inner")
    joined.write.mode("overwrite").parquet(f"{config.GOLD_PREFIX}/q1_meteo_pollution")
    pdf = joined.toPandas()
    print(f"  {len(pdf)} couples (ville, heure) croisés.")

    cols = ["temperature_c", "wind_speed_ms", "precipitation_mm",
            "pollution_indice", "pm25", "no2", "o3"]
    cols = [c for c in cols if c in pdf.columns]
    M = corr_heatmap(pdf, cols, "Q1 — Corrélations météo / pollution", "p4_q1_meteo_pollution.png")
    if M is not None and "pollution_indice" in M:
        print("  Corrélation de l'indice de pollution avec :")
        for v in ["temperature_c", "wind_speed_ms", "precipitation_mm"]:
            if v in M:
                print(f"    {v:18s} r = {M.loc['pollution_indice', v]:+.2f}")
    print("  -> Typiquement : vent = moins de pollution (dispersion), chaleur = plus d'ozone.")
