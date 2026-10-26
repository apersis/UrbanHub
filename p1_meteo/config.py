"""Configuration de la Partie 1 (météo). Connexion Spark/MinIO : voir `commun`."""
from __future__ import annotations

from commun.config import FIGURES_DIR, OUTPUT_DIR  # noqa: F401 (ré-export)

# --- Chemins des couches Medallion ------------------------------------------
BRONZE_PATH = "s3a://bronze/weather/global-hourly/"   # CSV ISD bruts
SILVER_PATH = "s3a://silver/weather/observations"     # Parquet typé/propre
GOLD_PREFIX = "s3a://gold/weather"                     # indicateurs métier

# --- Décodage ISD : (champ, position, sentinelle_à_nuller, facteur_d_échelle) ---
ISD_FIELDS = {
    "temperature_c":      ("TMP", 0, 9999,   10),   # °C
    "dewpoint_c":         ("DEW", 0, 9999,   10),   # °C
    "wind_direction_deg": ("WND", 0, 999,     1),   # degrés
    "wind_speed_ms":      ("WND", 3, 9999,   10),   # m/s
    "pressure_hpa":       ("SLP", 0, 99999,  10),   # hPa (niveau mer)
    "visibility_m":       ("VIS", 0, 999999,  1),   # mètres
    "precipitation_mm":   ("AA1", 1, 9999,   10),   # mm
}

# Bornes physiques de plausibilité (hors bornes -> null lors du nettoyage).
PHYSICAL_BOUNDS = {
    "temperature_c":    (-60, 60),
    "wind_speed_ms":    (0, 120),
    "pressure_hpa":     (850, 1100),
    "visibility_m":     (0, 160000),
    "precipitation_mm": (0, 500),
}

# Rattachement stations ISD -> grandes villes (par mots-clés du nom).
CITY_KEYWORDS = {
    "Paris":       ["ORLY", "MONTSOURIS", "BOURGET", "ROISSY", "GAULLE", "PARIS"],
    "Lyon":        ["LYON", "BRON", "SATOLAS", "EXUPERY"],
    "Marseille":   ["MARSEILLE", "MARIGNANE"],
    "Lille":       ["LESQUIN", "LILLE"],
    "Toulouse":    ["BLAGNAC", "TOULOUSE"],
    "Bordeaux":    ["MERIGNAC", "BORDEAUX"],
    "Nantes":      ["BOUGUENAIS", "NANTES"],
    "Strasbourg":  ["ENTZHEIM", "STRASBOURG"],
    "Nice":        ["NICE"],
    "Montpellier": ["FREJORGUES", "MONTPELLIER"],
}

# Seuils des événements extrêmes (Q4).
EXTREME_THRESHOLDS = {
    "canicule_tmax":      35,    # °C
    "grand_froid_tmin":  -10,    # °C
    "tempete_vent":       25,    # m/s (~90 km/h)
    "fortes_pluies_mm":   50,    # mm/jour
}

SEASON_ORDER = ["Hiver", "Printemps", "Été", "Automne"]
