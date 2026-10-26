#!/usr/bin/env python3
"""
UrbanHub - Partie 3 (Flux IoT) : collecte de la pollution atmosphérique (OpenAQ).

Simule un flux IoT : interroge régulièrement l'API OpenAQ v3 pour récupérer les
dernières mesures des capteurs situés en France (principales villes) et les dépose
dans la couche Bronze du Data Lake.

Source : https://api.openaq.org/v3  (nécessite une CLÉ API gratuite).
  -> créer un compte sur https://explore.openaq.org/ , récupérer la clé, puis :
       Windows PowerShell :  $env:OPENAQ_API_KEY = "votre_cle"
       (ou la définir dans l'environnement avant de lancer le script)

Bronze : bronze/pollution/openaq/date=YYYY-MM-DD/snapshot_<utc>.jsonl
Variables : sensor_id, pollutant, unit, value, latitude, longitude, timestamp
            (+ location_name, city, ingestion_timestamp)

Exemples
--------
    python -m p3_pollution.collecte                                  # France, toutes les 5 min
    python -m p3_pollution.collecte --interval 60 --iterations 3 --max-locations 50 --local-only
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from commun.env import charger_env

API = "https://api.openaq.org/v3"
BRONZE_BUCKET = "bronze"
BRONZE_PREFIX = "pollution/openaq"

# Polluants principaux demandés (noms de paramètres OpenAQ)
POLLUTANTS = {"pm25", "pm10", "no2", "o3", "co"}


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def session_with_key() -> requests.Session:
    key = os.getenv("OPENAQ_API_KEY")
    if not key:
        raise SystemExit("OPENAQ_API_KEY manquante. Créez une clé sur "
                         "https://explore.openaq.org/ puis définissez la variable "
                         "d'environnement OPENAQ_API_KEY.")
    s = requests.Session()
    s.headers.update({"X-API-Key": key})
    return s


def get_json(session: requests.Session, url: str, params: dict | None = None, retries: int = 6):
    """GET JSON avec retry/backoff respectant Retry-After (gère les 429)."""
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=60)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 2 * attempt)))
                continue
            r.raise_for_status()
            return r.json()
        except requests.HTTPError:
            raise
        except Exception:  # noqa: BLE001
            if attempt == retries:
                raise
            time.sleep(2 * attempt)
    raise requests.HTTPError(f"429 persistant sur {url}")


def france_country_id(session: requests.Session) -> int:
    for c in get_json(session, f"{API}/countries", {"limit": 300}).get("results", []):
        if c.get("code") == "FR":
            return c["id"]
    raise SystemExit("Pays France (FR) introuvable dans OpenAQ.")


def list_locations(session: requests.Session, country_id: int, cities: set[str] | None,
                   max_locations: int | None) -> list[dict]:
    """Localisations FR + carte sensorsId -> métadonnées polluant/position."""
    locations: list[dict] = []
    page = 1
    while True:
        results = get_json(session, f"{API}/locations",
                           {"countries_id": country_id, "limit": 1000, "page": page}).get("results", [])
        if not results:
            break
        locations.extend(results)
        if len(results) < 1000:
            break
        page += 1

    out = []
    for loc in locations:
        locality = (loc.get("locality") or loc.get("name") or "")
        if cities and not any(c.lower() in locality.lower() for c in cities):
            continue
        # ne garder que les capteurs de polluants d'intérêt
        sensors = {s["id"]: s for s in loc.get("sensors", [])
                   if (s.get("parameter") or {}).get("name") in POLLUTANTS}
        if sensors:
            loc["_sensors"] = sensors
            out.append(loc)
    if max_locations:
        out = out[:max_locations]
    return out


def fetch_latest(session: requests.Session, loc: dict, retries: int = 4) -> list[dict]:
    """Dernières mesures d'une localisation, mappées vers le schéma Bronze.

    Gère le 429 (limite de débit) avec un backoff respectant Retry-After.
    """
    ingestion_ts = dt.datetime.now(dt.timezone.utc).isoformat()
    results = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(f"{API}/locations/{loc['id']}/latest", timeout=60)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 2 * attempt)))
                continue
            r.raise_for_status()
            results = r.json().get("results", [])
            break
        except Exception as exc:  # noqa: BLE001
            if attempt == retries:
                log(f"  location {loc['id']} KO : {exc}")
                return []
            time.sleep(1.5 * attempt)
    if results is None:
        log(f"  location {loc['id']} KO : 429 persistant")
        return []
    sensors = loc["_sensors"]
    coords = loc.get("coordinates") or {}
    rows = []
    for m in results:
        sid = m.get("sensorsId")
        sensor = sensors.get(sid)
        if not sensor:
            continue  # capteur hors polluants d'intérêt
        param = sensor.get("parameter") or {}
        c = m.get("coordinates") or coords
        rows.append({
            "sensor_id": sid,
            "pollutant": param.get("name"),
            "unit": param.get("units"),
            "value": m.get("value"),
            "latitude": c.get("latitude"),
            "longitude": c.get("longitude"),
            "timestamp": (m.get("datetime") or {}).get("utc"),
            "location_name": loc.get("name"),
            "city": loc.get("locality"),
            "ingestion_timestamp": ingestion_ts,
        })
    return rows


def write_snapshot(rows, out_dir: Path, client) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    day = now.strftime("%Y-%m-%d")
    name = f"snapshot_{now.strftime('%Y%m%dT%H%M%SZ')}.jsonl"
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows).encode("utf-8")
    local = out_dir / f"date={day}" / name
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(payload)
    if client is not None:
        client.put_object(BRONZE_BUCKET, f"{BRONZE_PREFIX}/date={day}/{name}",
                          io.BytesIO(payload), len(payload), content_type="application/x-ndjson")
    log(f"  snapshot : {len(rows)} mesures -> {name}")


def make_minio_client():
    from minio import Minio
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    client = Minio(endpoint,
                   access_key=os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin")),
                   secret_key=os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")),
                   secure=False)
    if not client.bucket_exists(BRONZE_BUCKET):
        client.make_bucket(BRONZE_BUCKET)
    log(f"MinIO prêt sur {endpoint} (bucket '{BRONZE_BUCKET}')")
    return client


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Collecte IoT pollution OpenAQ (France) -> Bronze.")
    p.add_argument("--interval", type=int, default=300, help="Secondes entre snapshots (défaut 300 = 5 min).")
    p.add_argument("--iterations", type=int, default=None, help="Nombre de snapshots (défaut : illimité).")
    p.add_argument("--duration-min", type=float, default=None, help="Arrêt automatique après N minutes.")
    p.add_argument("--cities", nargs="+", default=None, help="Filtrer par villes (sous-chaîne de locality).")
    p.add_argument("--max-locations", type=int, default=150, help="Limiter le nombre de localisations.")
    p.add_argument("--workers", type=int, default=4,
                   help="Requêtes parallèles (défaut 4 : au-delà, risque de 429).")
    p.add_argument("--out", default="data/raw/openaq", help="Dossier cache local.")
    p.add_argument("--local-only", action="store_true", help="Ne pas uploader vers MinIO.")
    args = p.parse_args(argv)

    charger_env()           # charge .env en local (no-op si déjà dans l'environnement)
    session = session_with_key()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    cities = set(args.cities) if args.cities else None
    # Amorçage résilient : on réessaie tant que l'API ne répond pas (évite le crash-loop)
    locs = None
    while locs is None:
        try:
            cid = france_country_id(session)
            locs = list_locations(session, cid, cities, args.max_locations)
        except Exception as exc:  # noqa: BLE001
            log(f"OpenAQ indisponible ({exc}) ; nouvel essai dans 60s ...")
            time.sleep(60)
    log(f"{len(locs)} localisations ciblées (polluants {sorted(POLLUTANTS)})")
    if not locs:
        log("Aucune localisation à collecter.")
        return 1

    client = None if args.local_only else make_minio_client()

    deadline = (time.time() + args.duration_min * 60) if args.duration_min else None
    i = 0
    try:
        while args.iterations is None or i < args.iterations:
            i += 1
            t0 = time.time()
            log(f"Snapshot #{i} ...")
            rows: list[dict] = []
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = [pool.submit(fetch_latest, session, loc) for loc in locs]
                for fut in as_completed(futures):
                    rows.extend(fut.result())
            write_snapshot(rows, out_dir, client)
            if args.iterations is not None and i >= args.iterations:
                break
            if deadline is not None and time.time() >= deadline:
                break
            time.sleep(max(0, args.interval - (time.time() - t0)))
    except KeyboardInterrupt:
        log("Arrêt demandé (Ctrl+C).")
    log(f"Collecte terminée : {i} snapshot(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
