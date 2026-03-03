# Geopolitics & Oil Data Project

Analyse de la corrélation entre les événements géopolitiques et les prix du pétrole brut WTI.

**Équipe :** Omar Fekih-Hassen · Alexandre Donnat · Leo Ivars

---

## Vue d'ensemble

Ce projet implémente un **Data Lake end-to-end** qui :

1. Ingère les prix du pétrole WTI (15 min) depuis **Yahoo Finance** et les événements géopolitiques depuis l'API **GDELT v2**
2. Nettoie, filtre et enrichit les données avec **PySpark** (scores géopolitiques I/B/S)
3. Calcule un **indice de stress géopolitique** (Oil Stress Index) combinant les deux sources avec lissage par périodes de fermeture du marché
4. Indexe les résultats dans **Elasticsearch 8.10** pour visualisation dans **Kibana**
5. Déploie un **dashboard Kibana** automatisé via l'API Saved Objects

### Architecture

```
                          ┌─────────────┐
Yahoo Finance (15 min) ──►│             │
                          │  raw/ (S3)  │──► formatted/ (Silver) ──► combined/ (Gold) ──► Elasticsearch ──► Kibana
GDELT v2 (15 min)     ──►│  Parquet    │
                          └─────────────┘
```

Toutes les couches sont stockées dans un bucket **S3 local (LocalStack)** :

| Couche | Chemin S3 | Description |
|--------|-----------|-------------|
| **Raw** | `raw/gdelt/history/`, `raw/yahoofinance/history/` | Données brutes Parquet, 1 fichier par window 15 min |
| **Silver** | `formatted/gdelt/events.parquet`, `formatted/yahoofinance/wti.parquet` | Nettoyé, filtré, dédoublonné, enrichi (scores geo) |
| **Gold** | `combined/stress_index/` | Table ML-ready : WTI × GDELT jointé, stress lissé, percentile 7j |

---

## Prérequis

| Outil | Version minimale | Obligatoire |
|-------|-----------------|-------------|
| Python | 3.10+ | Oui |
| Poetry | 1.8+ | Oui |
| Docker & Docker Compose | 24+ | Oui |
| Java (JDK) | 11+ | Oui (pour PySpark) |
| Git | 2.40+ | Oui |
| AWS CLI (`awscli-local`) | 0.22+ | Optionnel |

---

## Installation

### 1. Cloner le dépôt

```bash
git clone https://github.com/omar7878/geopolitics-oil-data-project.git
cd geopolitics-oil-data-project
```

### 2. Installer les dépendances Python

```bash
poetry install
```

### 3. Lancer l'infrastructure Docker

```bash
cd infrastructure
docker compose up -d
cd ..
```

Attendez que tous les services soient healthy (~1 min) :

```bash
docker compose -f infrastructure/docker-compose.yml ps
```

Services disponibles :

| Service | URL | Description |
|---------|-----|-------------|
| LocalStack (S3) | http://localhost:4566 | Stockage objet S3 local |
| Elasticsearch | http://localhost:9200 | Moteur d'indexation (single-node, security off) |
| Kibana | http://localhost:5601 | Dashboard de visualisation |
| Airflow | http://localhost:8080 | Orchestration (admin / admin) |

---

## Utilisation

### Pipeline complet — Initialisation (à lancer une seule fois)

```bash
# ── Étape 1 — Ingestion Backfill (télécharge tout l'historique GDELT + WTI)
poetry run python -m src.ingestion.backfill_gdelt
poetry run python -m src.ingestion.backfill_yfinance

# ── Étape 2 — Formatting (nettoie, filtre, enrichit → couche Silver)
poetry run python -m src.transformation.clean_gdelt --mode history
poetry run python -m src.transformation.clean_yfinance --mode history

# ── Étape 3 — Calcul du Stress Index (join WTI × GDELT → couche Gold)
poetry run python -m src.combination.compute_stress_index --mode history

# ── Étape 4 — Indexation Elasticsearch
poetry run python -m src.indexing.load_to_elastic

# ── Étape 5 — Dashboard Kibana (Data View + 6 visualisations + dashboard)
poetry run python -m src.visualization.setup_kibana
```

### Pipeline quotidien (daily)

Remplacer `YYYY-MM-DD` par la date du jour :

```bash
poetry run python -m src.ingestion.batch_extract_gdelt --date YYYY-MM-DD && \
poetry run python -m src.ingestion.batch_extract_yfinance --date YYYY-MM-DD && \
poetry run python -m src.transformation.clean_gdelt --mode daily --date YYYY-MM-DD && \
poetry run python -m src.transformation.clean_yfinance --mode daily --date YYYY-MM-DD && \
poetry run python -m src.combination.compute_stress_index --mode daily --date YYYY-MM-DD && \
poetry run python -m src.indexing.load_to_elastic
```

> **Notes :**
> - En production, Airflow passe automatiquement `--date {{ ds }}` à chaque script.
> - Tous les scripts sont **idempotents** : relancer avec la même date écrase les données sans doublons.
> - `setup_kibana` n'est à lancer qu'une seule fois (ou après un reset de l'infra).

### Lancer via Airflow

1. Ouvrez http://localhost:8080 (admin / admin)
2. Activez le DAG `main_pipeline_dag`
3. Déclenchez une exécution manuelle ▶

---

## Elasticsearch & Kibana

### Index `oil-market-analysis`

Le script `load_to_elastic` crée l'index avec le mapping défini dans `config/elastic_mapping.json` :

| Champ | Type | Description |
|-------|------|-------------|
| `timestamp` | `date` | Datetime UTC de la bougie WTI (15 min) |
| `Open`, `High`, `Low`, `Close` | `float` | Prix OHLC du WTI (USD/bbl) |
| `Volume` | `long` | Volume de trading (contrats) |
| `Volatility_Range` | `float` | High − Low |
| `Variation_Pct` | `float` | Variation % vs bougie précédente |
| `geo_I_smoothed` | `float` | Importance géopolitique lissée |
| `geo_B_smoothed` | `float` | Buzz médiatique lissé |
| `geo_S_smoothed` | `float` | Sévérité lissée |
| `geo_score_raw_smoothed` | `float` | Score brut combiné lissé |
| `total_event_count` | `float` | Nombre d'événements GDELT cumulés |
| `gap_duration_15m` | `integer` | Durée de fermeture marché (en tranches 15 min) |
| `period_main_actor` | `keyword` | Acteur géopolitique dominant de la période |
| `score_pct_7d` | `float` | Percentile glissant 7 jours (Oil Stress Index) |

### Dashboard Kibana

Le script `setup_kibana` déploie automatiquement via l'API Saved Objects :

| Panneau | Type | Description |
|---------|------|-------------|
| WTI — Cours (Close) | Ligne | Prix de clôture WTI avec interpolation linéaire (weekends) |
| Stress Index (score_pct_7d) | Ligne | Percentile 7 jours du stress géopolitique |
| Prix WTI vs Stress Index | Double axe | Superposition prix / stress sur deux échelles |
| Volume de trading | Barres | Volume des contrats par période |
| Scores géopolitiques (smoothed) | Lignes | geo_I / geo_B / geo_S superposés |
| Top acteurs géopolitiques | Barres horiz. | Top 10 des `period_main_actor` par occurrences |

Accès : http://localhost:5601/app/dashboards

---

## Explorer les données avec les notebooks

```bash
poetry run jupyter notebook notebooks/
```

| Notebook | Contenu |
|----------|---------|
| `01_explore_raw_s3.ipynb` | Données brutes Parquet sur S3 |
| `02_explore_formatted_spark.ipynb` | Parquet après nettoyage Spark (couche Silver) |
| `03_explore_combined_stress_index.ipynb` | Stress Index, corrélations et visualisations Gold layer |
| `04_grid_search.ipynb` | Calibration de l'ALPHA optimal (grid search Spearman) |
| `05_grid_search.ipynb` | Grid search étendu sur les pondérations du score géopolitique |

---

## Vérifier S3 (optionnel)

```bash
# Avec awslocal
awslocal s3 ls s3://datalake/ --recursive

# Ou avec aws cli standard
aws --endpoint-url=http://localhost:4566 s3 ls s3://datalake/ --recursive
```

---

## Structure du projet

```
geopolitics-oil-data-project/
├── dags/                              # DAG Airflow (orchestration quotidienne)
│   └── main_pipeline_dag.py
├── src/
│   ├── ingestion/                     # Extraction des données brutes → S3
│   │   ├── backfill_gdelt.py          #   Historique complet GDELT (Master File List)
│   │   ├── backfill_yfinance.py       #   Historique complet WTI
│   │   ├── batch_extract_gdelt.py     #   Daily GDELT (96 fichiers/jour, --date)
│   │   └── batch_extract_yfinance.py  #   Daily WTI (--date)
│   ├── transformation/                # Nettoyage PySpark → couche Silver
│   │   ├── clean_gdelt.py             #   Filtre, dédoublonne, scores geo I/B/S
│   │   └── clean_yfinance.py          #   Cast, volatilité, variation %
│   ├── combination/                   # Fusion WTI × GDELT → couche Gold
│   │   └── compute_stress_index.py    #   Join, lissage fermeture, percentile 7j
│   ├── indexing/                      # Chargement Elasticsearch
│   │   └── load_to_elastic.py         #   Bulk index avec mapping explicite
│   └── visualization/                 # Dashboard Kibana
│       └── setup_kibana.py            #   Data View + 6 Lens + Dashboard
├── tests/                             # Tests unitaires (61 tests)
├── notebooks/                         # Exploration et prédiction (Jupyter)
├── infrastructure/
│   ├── docker-compose.yml             # LocalStack, Postgres, Airflow, ES, Kibana
│   ├── airflow.Dockerfile             # Image Docker custom pour Airflow
│   └── localstack_init.sh             # Création auto du bucket S3
├── config/
│   └── elastic_mapping.json           # Mapping Elasticsearch (20 champs)
├── pyproject.toml                     # Dépendances Poetry
└── ARCHITECTURE.md                    # Architecture détaillée
```

---

## Tests

```bash
poetry run pytest tests/ -v
```

Couverture des 8 modules du pipeline :

| Fichier de test | Module testé |
|---|---|
| `test_backfill_gdelt.py` | Ingestion historique GDELT |
| `test_backfill_yfinance.py` | Ingestion historique Yahoo Finance |
| `test_batch_extract_gdelt.py` | Extraction quotidienne GDELT |
| `test_batch_extract_yfinance.py` | Extraction quotidienne Yahoo Finance |
| `test_clean_gdelt.py` | Transformation Silver GDELT |
| `test_clean_yfinance.py` | Transformation Silver WTI |
| `test_compute_stress_index.py` | Calcul Gold layer |
| `test_load_to_elastic.py` | Indexation Elasticsearch |
| `test_setup_kibana.py` | Dashboard Kibana |

---

## Workflow Git

```bash
git checkout -b feat/nom-de-la-fonctionnalité
git add .
git commit -m "feat: description courte"
git push origin feat/nom-de-la-fonctionnalité
```

| Préfixe | Usage |
|---------|-------|
| `feat:` | Nouvelle fonctionnalité |
| `fix:` | Correction de bug |
| `refactor:` | Refactorisation |
| `test:` | Tests |
| `docs:` | Documentation |
| `chore:` | Maintenance |

---

## Documentation complémentaire

- [ARCHITECTURE.md](ARCHITECTURE.md) — Architecture détaillée du Data Lake
- [Yahoo Finance (CL=F)](https://finance.yahoo.com/quote/CL=F) — Prix du pétrole brut WTI
- [GDELT v2](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) — Documentation API événements géopolitiques
- [Elasticsearch 8.10](https://www.elastic.co/guide/en/elasticsearch/reference/8.10/index.html) — Documentation Elasticsearch
- [Kibana Saved Objects API](https://www.elastic.co/guide/en/kibana/8.10/saved-objects-api.html) — API utilisée par `setup_kibana.py`
