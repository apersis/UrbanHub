#!/usr/bin/env python3
"""
UrbanHub - Partie 1 (Flux Batch) : ingestion des donnees meteo NOAA.

Telecharge EN PARALLELE les fichiers horaires "Global Hourly" (ISD) des stations
meteorologiques situees en France metropolitaine, sur les N dernieres annees, et
les depose BRUTS dans la couche Bronze du Data Lake (MinIO, bucket "bronze").

Source de donnees
-----------------
NOAA Global Hourly Weather Data (Integrated Surface Database, ISD).
Deux miroirs du meme jeu de donnees :
  * AWS Open Data (par defaut, fiable, sans proxy)    : bucket S3 "noaa-global-hourly-pds"
      https://noaa-global-hourly-pds.s3.amazonaws.com/{annee}/{station}.csv
  * NCEI (le lien des consignes, parfois en maintenance / 503) :
      https://www.ncei.noaa.gov/data/global-hourly/access/{annee}/{station}.csv

Identification des stations francaises
--------------------------------------
Dans l'ISD, l'identifiant station = USAF(6) + WBAN(5). Les stations synoptiques de
France metropolitaine appartiennent au bloc OMM 07xxx : leur identifiant commence
donc par "07". On liste les objets du bucket avec le prefixe "{annee}/07" : rapide,
fiable, et sans dependre du fichier isd-history.csv (souvent indisponible).
Les territoires d'outre-mer utilisent d'autres blocs (ex. 61=Reunion, 78=Antilles) :
ajouter leurs prefixes via --prefix si besoin.

Exemples
--------
    # Ingestion complete -> Bronze
    python -m p1_meteo.collecte

    # Test rapide : 10 stations, 2 annees, sans upload MinIO (cache local seulement)
    python -m p1_meteo.collecte --years 2024 2023 --max-stations 10 --local-only

    # Outre-mer inclus
    python -m p1_meteo.collecte --prefix 07 61 78 89
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from commun.env import charger_env

# --- Miroirs de donnees -----------------------------------------------------
S3_BUCKET_URL = "https://noaa-global-hourly-pds.s3.amazonaws.com"
NCEI_ACCESS_URL = "https://www.ncei.noaa.gov/data/global-hourly/access"
S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"  # namespace XML des listings S3

# --- Data Lake (Bronze) -----------------------------------------------------
BRONZE_BUCKET = "bronze"
BRONZE_PREFIX = "weather/global-hourly"  # bronze/weather/global-hourly/year=YYYY/STATION.csv


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def default_years(n: int = 5) -> list[int]:
    """Les N dernieres annees pleines (hors annee courante, incomplete)."""
    y = dt.date.today().year
    return list(range(y - n, y))


# ---------------------------------------------------------------------------
# 1) Listing des stations francaises presentes pour une annee donnee
# ---------------------------------------------------------------------------
def list_station_keys(session: requests.Session, year: int, prefixes: list[str]) -> list[str]:
    """Retourne les cles S3 ('{year}/{station}.csv') des stations FR pour l'annee."""
    keys: list[str] = []
    for pref in prefixes:
        token = None
        while True:
            params = {"list-type": "2", "prefix": f"{year}/{pref}", "max-keys": "1000"}
            if token:
                params["continuation-token"] = token
            r = session.get(S3_BUCKET_URL, params=params, timeout=60)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            for c in root.findall(f"{S3_NS}Contents"):
                k = c.findtext(f"{S3_NS}Key")
                if k and k.endswith(".csv"):
                    keys.append(k)
            if root.findtext(f"{S3_NS}IsTruncated") == "true":
                token = root.findtext(f"{S3_NS}NextContinuationToken")
            else:
                break
    return sorted(set(keys))


# ---------------------------------------------------------------------------
# 2) Telechargement d'un fichier (avec retries) -> cache local
# ---------------------------------------------------------------------------
def download_one(session: requests.Session, key: str, out_dir: Path, retries: int = 3) -> tuple[str, Path | None, int]:
    """Telecharge une cle vers le cache local. Retourne (key, chemin|None, taille_octets)."""
    local = out_dir / key  # out_dir/{year}/{station}.csv
    if local.exists() and local.stat().st_size > 0:
        return key, local, local.stat().st_size  # deja en cache -> idempotent
    local.parent.mkdir(parents=True, exist_ok=True)
    url = f"{S3_BUCKET_URL}/{key}"
    for attempt in range(1, retries + 1):
        try:
            with session.get(url, timeout=120, stream=True) as r:
                if r.status_code == 404:
                    return key, None, 0  # station absente cette annee-la
                r.raise_for_status()
                tmp = local.with_suffix(".csv.part")
                size = 0
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 16):
                        fh.write(chunk)
                        size += len(chunk)
                tmp.replace(local)
                return key, local, size
        except Exception as exc:  # noqa: BLE001
            if attempt == retries:
                log(f"  ECHEC {key} : {exc}")
                return key, None, 0
            time.sleep(1.5 * attempt)
    return key, None, 0


# ---------------------------------------------------------------------------
# 3) Upload du cache local vers MinIO (Bronze)
# ---------------------------------------------------------------------------
def make_minio_client():
    from minio import Minio  # import tardif : dependance optionnelle

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    client = Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin")),
        secret_key=os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")),
        secure=False,
    )
    if not client.bucket_exists(BRONZE_BUCKET):
        client.make_bucket(BRONZE_BUCKET)
    log(f"MinIO pret sur {endpoint} (bucket '{BRONZE_BUCKET}')")
    return client


def upload_one(client, key: str, local: Path) -> bool:
    """Depose le CSV brut dans bronze/weather/global-hourly/year=YYYY/STATION.csv."""
    year, station = key.split("/", 1)  # "2024", "07100099999.csv"
    object_name = f"{BRONZE_PREFIX}/year={year}/{station}"
    try:
        client.fput_object(BRONZE_BUCKET, object_name, str(local), content_type="text/csv")
        return True
    except Exception as exc:  # noqa: BLE001
        log(f"  UPLOAD KO {object_name} : {exc}")
        return False


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingestion batch NOAA Global Hourly -> Bronze (France).")
    p.add_argument("--years", type=int, nargs="+", default=default_years(5),
                   help="Annees a ingerer (defaut : 5 dernieres annees pleines).")
    p.add_argument("--prefix", nargs="+", default=["07"],
                   help="Prefixes OMM des stations (defaut 07 = France metropolitaine).")
    p.add_argument("--max-stations", type=int, default=None,
                   help="Limite le nombre de stations (test rapide).")
    p.add_argument("--workers", type=int, default=16, help="Telechargements paralleles.")
    p.add_argument("--out", default="data/raw/global-hourly", help="Dossier cache local.")
    p.add_argument("--local-only", action="store_true", help="Ne pas uploader vers MinIO.")
    args = p.parse_args(argv)

    charger_env()           # charge .env en local (no-op si déjà dans l'environnement)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()

    log(f"Annees : {args.years} | prefixes : {args.prefix} | workers : {args.workers}")

    # 1) Construire la liste des stations FR a telecharger (toutes annees confondues)
    log("Listing des stations francaises sur le bucket NOAA ...")
    all_keys: list[str] = []
    stations_par_an: dict[int, int] = {}
    for year in sorted(args.years):
        keys = list_station_keys(session, year, args.prefix)
        if args.max_stations:
            keys = keys[: args.max_stations]
        stations_par_an[year] = len(keys)
        all_keys.extend(keys)
        log(f"  {year} : {len(keys)} stations")
    log(f"Total : {len(all_keys)} fichiers (station x annee) a telecharger.")

    # 2) Telechargement parallele -> cache local
    t0 = time.time()
    results: list[tuple[str, Path | None, int]] = []
    ok = ko = total_bytes = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(download_one, session, k, out_dir) for k in all_keys]
        for i, fut in enumerate(as_completed(futures), 1):
            key, local, size = fut.result()
            results.append((key, local, size))
            if local:
                ok += 1
                total_bytes += size
            else:
                ko += 1
            if i % 50 == 0 or i == len(futures):
                log(f"  telecharges {i}/{len(futures)} (ok={ok}, absents/echecs={ko})")
    log(f"Telechargement termine en {time.time() - t0:.0f}s : "
        f"{ok} fichiers, {total_bytes / 1e6:.1f} Mo (cache : {out_dir}).")

    # 3) Upload vers MinIO (Bronze)
    if args.local_only:
        log("Mode --local-only : pas d'upload MinIO.")
    else:
        try:
            client = make_minio_client()
        except Exception as exc:  # noqa: BLE001
            log(f"MinIO indisponible ({exc}). Donnees conservees en cache local uniquement.")
            return 0
        up_ok = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(upload_one, client, k, lp): k
                       for (k, lp, _) in results if lp is not None}
            for i, fut in enumerate(as_completed(futures), 1):
                if fut.result():
                    up_ok += 1
                if i % 50 == 0 or i == len(futures):
                    log(f"  uploades {i}/{len(futures)}")
        log(f"Upload Bronze termine : {up_ok} objets dans "
            f"s3a://{BRONZE_BUCKET}/{BRONZE_PREFIX}/year=YYYY/STATION.csv")

    log("Ingestion Partie 1 terminee.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
