# UrbanHub — Data Lake (jumeau numérique urbain)

Data Lake **Medallion** (bronze/silver/gold) sur **MinIO** + **Spark**, en fichiers Python
séparés (pas de notebook). Exécution : **Python local + Spark dans Docker (Spark Connect)**.

- **Partie 1 — Batch (météo)** : `p1_meteo/`
- **Partie 2 — Streaming (vélos)** : `p2_mobilite/`
- **Partie 3 — IoT (pollution)** : `p3_pollution/`
- **Partie 4 — Analyse croisée** : `p4_croisement/`
- Code partagé (connexion Spark/MinIO) : `commun/`

Chaque partie suit le même schéma : `collecte.py` (→ Bronze) · `nettoyage.py` (Bronze→Silver) ·
`pipeline.py` (orchestration) · `analyses/qN_*.py` (→ Gold + figures).

## Visualiser les données

- **Graphiques** : chaque pipeline d'analyse écrit des PNG dans **`output/figures/`**
  (`p1_*` = météo, `p2_*` = vélos, `p3_*` = pollution, `p4_*` = croisé).
- **Data Lake (console MinIO)** : http://localhost:9001 (login `minioadmin` / `minioadmin123`) —
  parcourir/télécharger les buckets `bronze` / `silver` / `gold`.
- **Spark UI** : http://localhost:4040 pendant un job.
- **Lire une table Gold** (ex.) :
  ```bash
  python -c "from commun.spark import build_spark; build_spark().read.parquet('s3a://gold/weather/q3_temp_mensuelle_nationale').show()"
  ```

## Mise en route (commune à toutes les parties)

Pré-requis client : **Python 64-bit 3.10 / 3.11 / 3.12** (PySpark ne supporte pas 3.13/3.14).

```bash
cd UrbanHub
cp .env.exemple .env            # identifiants MinIO (+ OPENAQ_API_KEY pour la Partie 3)

docker compose up -d            # Spark + MinIO (1er run : télécharge les JARs -> attendre "spark healthy")

py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1    # PowerShell  (Linux/Mac : source .venv/bin/activate)
python -m pip install -r requirements.txt
```

> Serveur Spark épinglé à **3.5.0** (`apache/spark:3.5.0`) → client `pyspark` en 3.5.x
> (déjà fixé dans `requirements.txt`). Activer `.venv` à chaque session.

---

# Partie 1 — Batch (météo urbaine)

Source : **NOAA Global Hourly** (ISD), stations de France (bloc OMM `07`), via AWS Open Data.

```bash
python -m p1_meteo.collecte --years 2024 2023 --max-stations 10   # ingestion -> Bronze
python -m p1_meteo.pipeline                                       # Silver + Gold + analyses
```

Options collecte : `--years`, `--max-stations`, `--workers`, `--local-only` ·
`pipeline --step silver|analyses --question q2`.

Questions (`p1_meteo/analyses/`) : **Q1** périodes anormales · **Q2** corrélation météo/visibilité ·
**Q3** saisonnalité de la température · **Q4** jours extrêmes.

---

# Partie 2 — Streaming (mobilité vélos)

Source : **API CityBikes** (réseaux FR), collecte continue toutes les ~2-3 min.

```bash
# Collecte -> Bronze (Ctrl+C, ou --iterations N / --duration-min 30)
python -m p2_mobilite.collecte --networks velib bicloo v3-bordeaux
# En arrière-plan (service Docker)
docker compose --profile velos up -d        # logs: docker compose logs -f collector-citybikes

python -m p2_mobilite.pipeline              # Silver + Gold + analyses
```

> L'API limite le débit (429) : garder `--workers` bas (défaut 4) et cibler quelques réseaux
> (géré par retry/backoff).

Questions (`p2_mobilite/analyses/`) : **Q1** stations les plus utilisées · **Q2** offre insuffisante ·
**Q3** pics journaliers · **Q4** déséquilibres géographiques · **Q5** stations à rééquilibrer.

---

# Partie 3 — IoT (pollution urbaine)

Source : **API OpenAQ v3** (capteurs FR ; PM2.5, PM10, NO2, O3, CO).

> ⚠️ **Clé API gratuite requise** (https://explore.openaq.org/) : renseigner
> `OPENAQ_API_KEY=` dans `.env` (chargé automatiquement en local comme pour Docker).

```bash
python -m p3_pollution.collecte --iterations 3 --max-locations 50   # collecte -> Bronze
docker compose --profile pollution up -d                           # ou en arrière-plan

python -m p3_pollution.pipeline                                    # Silver + Gold + analyses
```

Questions (`p3_pollution/analyses/`) : **Q1** zones les plus polluées · **Q2** variation journalière ·
**Q3** polluant dominant · **Q4** épisodes anormaux.

---

# Partie 4 — Analyse croisée (météo × mobilité × pollution)

Croise les couches Silver des parties 1/2/3 sur des **profils (ville × heure)** — les flux
couvrant des périodes différentes, on aligne par ville et heure de la journée.

```bash
python -m p4_croisement.pipeline               # (--question q2 pour une seule)
```

Questions (`p4_croisement/analyses/`) : **Q1** météo ↔ pollution · **Q2** météo ↔ vélos ·
**Q3** conditions favorables à la mobilité douce · **Q4** périodes de forte interaction.

---

## Arborescence

```
UrbanHub/
├── compose.yaml            MinIO + Spark Connect + collecteurs (profils velos/pollution)
├── requirements.txt        deps client local
├── commun/                 connexion partagée : config.py (Spark/MinIO) · spark.py
├── p1_meteo/               Partie 1 — météo
│   ├── config.py · collecte.py · nettoyage.py · pipeline.py
│   └── analyses/q1..q4.py
├── p2_mobilite/            Partie 2 — vélos   (collecte · nettoyage · pipeline · analyses/q1..q5)
├── p3_pollution/           Partie 3 — pollution (collecte · nettoyage · pipeline · analyses/q1..q4)
└── p4_croisement/          Partie 4 — croisé  (profils.py · pipeline.py · analyses/q1..q4)
```
