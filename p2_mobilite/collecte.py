#!/usr/bin/env python3
"""
UrbanHub - Partie 2 (Flux Streaming) : collecte des vélos en libre-service.

Interroge l'API CityBikes toutes les 2-3 minutes et dépose chaque instantané
(snapshot) des stations des réseaux FRANÇAIS dans la couche Bronze du Data Lake.
C'est le "flux streaming" : une acquisition continue, snapshot après snapshot.

Source : https://api.citybik.es/v2/networks  (puis .../networks/{id} par réseau).

Bronze : bronze/mobility/citybikes/date=YYYY-MM-DD/snapshot_<utc>.jsonl
         (JSON Lines : une ligne = une station à un instant t)

Exemples
--------
    # Collecte continue toutes les 2,5 min (Ctrl+C pour arrêter)
    python -m p2_mobilite.collecte

    # 5 snapshots espacés de 30 s sur 2 réseaux, sans upload (test)
    python -m p2_mobilite.collecte --networks velib bicloo --interval 30 --iterations 5 --local-only
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

API = "https://api.citybik.es/v2"
BRONZE_BUCKET = "bronze"
BRONZE_PREFIX = "mobility/citybikes"

# Variables demandées (consigne) + contexte réseau/ville pour l'analyse
FIELDS = ["network_id", "network_name", "city", "station_id", "station_name",
          "latitude", "longitude", "bikes_available", "free_slots",
          "station_timestamp", "ingestion_timestamp"]


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def get_json(session: requests.Session, url: str, retries: int = 6):
    """GET JSON avec retry/backoff respectant Retry-After (gère les 429)."""
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=60)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 2 * attempt)))
                continue
            r.raise_for_status()
            return r.json()
        except requests.HTTPError:
            raise
        except Exception:  # noqa: BLE001  (réseau : on réessaie)
            if attempt == retries:
                raise
            time.sleep(2 * attempt)
    raise requests.HTTPError(f"429 persistant sur {url}")


def list_fr_networks(session: requests.Session) -> list[dict]:
    """Réseaux CityBikes situés en France (country == 'FR')."""
    nets = get_json(session, f"{API}/networks")["networks"]
    return [n for n in nets if n.get("location", {}).get("country") == "FR"]


def fetch_network(session: requests.Session, net: dict, retries: int = 4) -> list[dict]:
    """Récupère les stations d'un réseau et les mappe vers le schéma Bronze.

    Gère le 429 (Too Many Requests) de l'API CityBikes avec un backoff respectant
    l'en-tête Retry-After.
    """
    ingestion_ts = dt.datetime.now(dt.timezone.utc).isoformat()
    data = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(f"{API}/networks/{net['id']}", timeout=60)
            if r.status_code == 429:  # limite de débit atteinte -> on patiente
                wait = int(r.headers.get("Retry-After", 2 * attempt))
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()["network"]
            break
        except Exception as exc:  # noqa: BLE001
            if attempt == retries:
                log(f"  réseau {net['id']} KO : {exc}")
                return []
            time.sleep(1.5 * attempt)
    if data is None:
        log(f"  réseau {net['id']} KO : 429 persistant")
        return []
    city = (data.get("location") or {}).get("city")
    rows = []
    for s in data.get("stations", []):
        rows.append({
            "network_id": data["id"],
            "network_name": data.get("name"),
            "city": city,
            "station_id": s.get("id"),
            "station_name": s.get("name"),
            "latitude": s.get("latitude"),
            "longitude": s.get("longitude"),
            "bikes_available": s.get("free_bikes"),   # vélos dispo
            "free_slots": s.get("empty_slots"),       # places libres
            "station_timestamp": s.get("timestamp"),
            "ingestion_timestamp": ingestion_ts,
        })
    return rows


def take_snapshot(session, networks, workers) -> list[dict]:
    """Un instantané complet : stations de tous les réseaux ciblés."""
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch_network, session, n) for n in networks]
        for fut in as_completed(futures):
            rows.extend(fut.result())
    return rows


def write_snapshot(rows, out_dir: Path, client) -> None:
    """Écrit un snapshot en JSONL (cache local) puis, si demandé, dans Bronze."""
    now = dt.datetime.now(dt.timezone.utc)
    day = now.strftime("%Y-%m-%d")
    name = f"snapshot_{now.strftime('%Y%m%dT%H%M%SZ')}.jsonl"
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows).encode("utf-8")

    local = out_dir / f"date={day}" / name
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(payload)

    if client is not None:
        object_name = f"{BRONZE_PREFIX}/date={day}/{name}"
        client.put_object(BRONZE_BUCKET, object_name, io.BytesIO(payload), len(payload),
                          content_type="application/x-ndjson")
    log(f"  snapshot : {len(rows)} stations -> {name}")


def make_minio_client():
    from minio import Minio
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    client = Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin")),
        secret_key=os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")),
        secure=False,
    )
    if not client.bucket_exists(BRONZE_BUCKET):
        client.make_bucket(BRONZE_BUCKET)
    log(f"MinIO prêt sur {endpoint} (bucket '{BRONZE_BUCKET}')")
    return client


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Collecte streaming CityBikes (France) -> Bronze.")
    p.add_argument("--interval", type=int, default=150, help="Secondes entre snapshots (défaut 150 = 2,5 min).")
    p.add_argument("--iterations", type=int, default=None, help="Nombre de snapshots (défaut : illimité, Ctrl+C).")
    p.add_argument("--duration-min", type=float, default=None, help="Arrêt automatique après N minutes.")
    p.add_argument("--networks", nargs="+", default=None, help="IDs de réseaux à cibler (défaut : tous les réseaux FR).")
    p.add_argument("--workers", type=int, default=4,
                   help="Requêtes réseau parallèles (défaut 4 : au-delà, l'API renvoie des 429).")
    p.add_argument("--out", default="data/raw/citybikes", help="Dossier cache local.")
    p.add_argument("--local-only", action="store_true", help="Ne pas uploader vers MinIO.")
    args = p.parse_args(argv)

    charger_env()           # charge .env en local (no-op si déjà dans l'environnement)
    session = requests.Session()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Amorçage résilient : on réessaie tant que l'API ne répond pas (évite le crash-loop)
    fr = None
    while fr is None:
        try:
            fr = list_fr_networks(session)
        except Exception as exc:  # noqa: BLE001
            log(f"Liste des réseaux indisponible ({exc}) ; nouvel essai dans 60s ...")
            time.sleep(60)
    if args.networks:
        wanted = set(args.networks)
        fr = [n for n in fr if n["id"] in wanted]
    log(f"{len(fr)} réseaux ciblés : {', '.join(n['id'] for n in fr[:12])}{' ...' if len(fr) > 12 else ''}")
    if not fr:
        log("Aucun réseau à collecter (vérifier --networks).")
        return 1

    client = None if args.local_only else make_minio_client()

    deadline = (time.time() + args.duration_min * 60) if args.duration_min else None
    i = 0
    try:
        while args.iterations is None or i < args.iterations:
            i += 1
            t0 = time.time()
            log(f"Snapshot #{i} ...")
            rows = take_snapshot(session, fr, args.workers)
            write_snapshot(rows, out_dir, client)
            # Conditions d'arrêt
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
