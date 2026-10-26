"""Chargement d'un fichier .env (sans dépendance externe).

Utile pour les exécutions LOCALES : `.env` n'est lu automatiquement que par Docker
Compose. En local, on peuple os.environ depuis .env (sans écraser les variables
déjà définies, pour que l'environnement Docker/shell reste prioritaire).
"""
from __future__ import annotations

import os
from pathlib import Path


def charger_env(path: str = ".env") -> None:
    p = Path(path)
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        cle, _, valeur = line.partition("=")
        cle = cle.strip()
        valeur = valeur.strip().strip('"').strip("'")
        if cle:
            os.environ.setdefault(cle, valeur)
