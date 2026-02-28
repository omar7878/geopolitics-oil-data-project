# Geopolitics & Oil Data Project

Analyse de la corrélation entre les événements géopolitiques et les prix du pétrole brut WTI.

**Équipe :** Omar Fekih-Hassen · Alexandre Donnat · Leo Ivars

---

## Vue d'ensemble

Ce projet implémente un **Data Lake end-to-end** qui :

1. Ingère les prix du pétrole WTI depuis **Yahoo Finance** et les événements géopolitiques depuis l'API **GDELT**
2. Nettoie et normalise les données avec **PySpark**
3. Calcule un **indice de stress géopolitique** combinant les deux sources
4. Indexe les résultats dans **Elasticsearch** pour visualisation dans **Kibana**

### Architecture

```
Yahoo Finance ──┐
                ├──► raw/ (Parquet) ──► formatted/ (Parquet) ──► combined/ (Parquet) ──► Elasticsearch ──► Kibana
API GDELT    ──┘
```

Toutes les couches sont stockées dans un bucket **S3 local (LocalStack)**.

---

## Prérequis

| Outil | Version minimale | Obligatoire |
|-------|-----------------|-------------|
| Python | 3.10+ | Oui |
| Poetry | 1.8+ | Oui |
| Docker & Docker Compose | 24+ | Oui |
| Git | 2.40+ | Oui |
| AWS CLI (`awscli-local`) | 0.22+ | Optionnel |

---

## Installation

### 1. Cloner le dépôt

```bash
git clone https://github.com/omar7878/geopolitics-oil-data-project.git
cd geopolitics-oil-data-project
```

### 2. Configurer les variables d'environnement

```bash
cp .env.example .env
```

> Les variables d'environnement (LocalStack, Elasticsearch, Airflow) sont déjà configurées pour l'environnement local.
> Yahoo Finance ne nécessite pas de clé API.

### 3. Installer les dépendances Python

```bash
poetry install
```

Pour activer l'environnement virtuel :

```bash
source .venv/bin/activate
```

### 4. Installer l'AWS CLI locale (optionnel)

`awscli-local` fournit la commande `awslocal` qui pointe automatiquement vers LocalStack — aucune configuration AWS n'est requise :

```bash
poetry install   # installe awscli-local avec les autres dépendances
```

Commandes utiles :

```bash
# Lister les buckets
awslocal s3 ls

# Lister le contenu du Data Lake
awslocal s3 ls s3://datalake/ --recursive

# Télécharger un fichier depuis S3
awslocal s3 cp s3://datalake/raw/yahoofinance/2024-01-01/wti_price.json .
```

> **Note :** `awslocal` est un wrapper autour d'`aws` qui ajoute `--endpoint-url=http://localhost:4566` automatiquement.

### 5. Lancer l'infrastructure Docker

```bash
cd infrastructure
docker compose up -d 
cd ..
```

Attendez que tous les services soient healthy (environ 1 minute) :

```bash
docker compose -f infrastructure/docker-compose.yml ps
```

Services disponibles :

| Service | URL |
|---------|-----|
| Airflow | http://localhost:8080 (admin / admin) |
| Kibana | http://localhost:5601 |
| Elasticsearch | http://localhost:9200 |
| LocalStack (S3) | http://localhost:4566 |

---

## Utilisation

### Lancer le pipeline complet via Airflow

1. Ouvrez Airflow : http://localhost:8080
2. Activez le DAG `main_pipeline_dag`
3. Déclenchez une exécution manuelle via le bouton ▶

### Vérifier que S3 fonctionne (optionnel)

```bash
# Depuis votre machine (awslocal)
awslocal s3 ls s3://datalake/

# Ou depuis le conteneur Airflow
docker exec airflow_webserver awslocal s3 ls s3://datalake/
```

### Lancer les étapes manuellement

```bash
# ── Étape 1a — Ingestion Backfill (historique initial, à lancer une seule fois)
poetry run python -m src.ingestion.backfill_yfinance
poetry run python -m src.ingestion.backfill_gdelt

# ── Étape 1b — Ingestion Daily (idempotente, pilotée par --date)
poetry run python -m src.ingestion.batch_extract_yfinance --date 2026-02-27
poetry run python -m src.ingestion.batch_extract_gdelt --date 2026-02-27

# ── Étape 2a — Formatting History (init unique du fichier Silver)
poetry run python -m src.transformation.clean_yfinance --mode history
poetry run python -m src.transformation.clean_gdelt --mode history

# ── Étape 2b — Formatting Daily (incrémental, pilotée par --date)
poetry run python -m src.transformation.clean_yfinance --mode daily --date 2026-02-27
poetry run python -m src.transformation.clean_gdelt --mode daily --date 2026-02-27

# ── Étape 3 — Calcul du Stress Index
poetry run python -m src.combination.compute_stress_index

# ── Étape 4 — Indexation Elasticsearch
poetry run python -m src.indexing.load_to_elastic
```

> **Note :** En production, Airflow passe automatiquement `--date {{ ds }}` à chaque script.
> Les scripts d'ingestion et de formatting sont **idempotents** : relancer avec la même date écrase les données existantes sans créer de doublons.

### Explorer les données avec les notebooks

Lancez Jupyter :

```bash
poetry run jupyter notebook notebooks/
```

| Notebook | Contenu |
|----------|---------|
| `01_explore_raw_s3.ipynb` | Données brutes Parquet sur S3 |
| `02_explore_formatted_spark.ipynb` | Parquet après nettoyage Spark (couche Silver) |
| `03_explore_combined_stress_index.ipynb` | Stress Index et corrélations |
| `04_explore_elasticsearch.ipynb` | Requêtes Elasticsearch |

---

## Structure du projet

```
geopolitics-oil-data-project/
├── dags/                         # DAGs Airflow (orchestration)
├── src/
│   ├── ingestion/                # Extraction des données (Yahoo Finance, GDELT)
│   │   ├── backfill_yfinance.py  #   Backfill historique WTI (une seule fois)
│   │   ├── backfill_gdelt.py     #   Backfill historique GDELT (une seule fois)
│   │   ├── batch_extract_yfinance.py  # Daily idempotent WTI (--date)
│   │   └── batch_extract_gdelt.py     # Daily idempotent GDELT (--date)
│   ├── transformation/           # Nettoyage PySpark (couche Silver)
│   │   ├── clean_yfinance.py     #   --mode history | --mode daily --date
│   │   └── clean_gdelt.py        #   --mode history | --mode daily --date
│   ├── combination/              # Calcul du Stress Index
│   └── indexing/                 # Chargement Elasticsearch
├── tests/                        # Tests unitaires (pytest)
├── notebooks/                    # Exploration et visualisation
├── infrastructure/               # Docker Compose + init S3
│   ├── docker-compose.yml        # LocalStack, Postgres, Airflow, Elastic, Kibana
│   └── localstack_init.sh        # Création auto du bucket S3 au démarrage
├── config/
│   └── elastic_mapping.json      # Mapping Elasticsearch
├── pyproject.toml                # Dépendances Poetry
└── ARCHITECTURE.md               # Architecture détaillée
```

---

## Tests

```bash
poetry run pytest tests/ -v
```

---

## Workflow Git

Nous travaillons avec des branches par fonctionnalité :

```bash
# Créer sa branche
git checkout -b feat/nom-de-la-fonctionnalité

# Commiter
git add .
git commit -m "feat: description courte de ce qui a été fait"

# Pousser et ouvrir une PR
git push origin feat/nom-de-la-fonctionnalité
```

### Conventions de commits

| Préfixe | Usage |
|---------|-------|
| `feat:` | Nouvelle fonctionnalité |
| `fix:` | Correction de bug |
| `refactor:` | Refactorisation sans changement de comportement |
| `test:` | Ajout ou modification de tests |
| `docs:` | Documentation uniquement |
| `chore:` | Maintenance (dépendances, config…) |

> Ne jamais commiter `.env` — il contient des secrets.

---

## Documentation complémentaire

- [ARCHITECTURE.md](ARCHITECTURE.md) — Architecture détaillée du Data Lake
- [Yahoo Finance](https://finance.yahoo.com/quote/CL=F) — Prix du pétrole brut WTI (Ticker: CL=F)
- [yfinance Documentation](https://pypi.org/project/yfinance/) — Bibliothèque Python pour Yahoo Finance
- [GDELT API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) — Documentation API événements géopolitiques
