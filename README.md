# Geopolitics & Oil Data Project

Analyse de la corrélation entre les événements géopolitiques et les prix du pétrole brut WTI.

**Équipe :** Omar Fekih-Hassen · Alexandre Donnat · Leo Ivars

---

## Vue d'ensemble

Ce projet implémente un **Data Lake end-to-end** qui :

1. Ingère les prix du pétrole WTI depuis l'API **FRED** et les événements géopolitiques depuis l'API **GDELT**
2. Nettoie et normalise les données avec **PySpark**
3. Calcule un **indice de stress géopolitique** combinant les deux sources
4. Indexe les résultats dans **Elasticsearch** pour visualisation dans **Kibana**

### Architecture

```
API FRED  ──┐
             ├──► raw/ (JSON) ──► formatted/ (Parquet) ──► combined/ (Parquet) ──► Elasticsearch ──► Kibana
API GDELT ──┘
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

Ouvrez `.env` et renseignez votre clé API FRED :

```
FRED_API_KEY=votre_clé_ici
```

> Obtenez une clé gratuite sur [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html)

Les autres variables (LocalStack, Elasticsearch, Airflow) sont déjà configurées pour l'environnement local.

### 3. Installer les dépendances Python

```bash
poetry install
```

Pour activer l'environnement virtuel :

```bash
source .env/bin/activate
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
awslocal s3 cp s3://datalake/raw/fred/2024-01-01/wti_price.json .
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
# Étape 1 — Ingestion FRED
poetry run python src/ingestion/batch_extract_fred.py

# Étape 1 — Ingestion GDELT
poetry run python src/ingestion/batch_extract_gdelt.py

# Étape 2 — Nettoyage (Spark)
poetry run python src/transformation/clean_fred.py
poetry run python src/transformation/clean_gdelt.py

# Étape 3 — Calcul du Stress Index
poetry run python src/combination/compute_stress_index.py

# Étape 4 — Indexation Elasticsearch
poetry run python src/indexing/load_to_elastic.py
```

### Explorer les données avec les notebooks

Lancez Jupyter :

```bash
poetry run jupyter notebook notebooks/
```

| Notebook | Contenu |
|----------|---------|
| `01_explore_raw_s3.ipynb` | Données brutes JSON sur S3 |
| `02_explore_formatted_spark.ipynb` | Parquet après nettoyage Spark |
| `03_explore_combined_stress_index.ipynb` | Stress Index et corrélations |
| `04_explore_elasticsearch.ipynb` | Requêtes Elasticsearch |

---

## Structure du projet

```
geopolitics-oil-data-project/
├── dags/                         # DAGs Airflow (orchestration)
├── src/
│   ├── ingestion/                # Extraction des données (FRED, GDELT)
│   ├── transformation/           # Nettoyage Spark (JSON → Parquet)
│   ├── combination/              # Calcul du Stress Index
│   └── indexing/                 # Chargement Elasticsearch
├── tests/                        # Tests unitaires (pytest)
├── notebooks/                    # Exploration et visualisation
├── infrastructure/               # Docker Compose + init S3
├── config/
│   └── elastic_mapping.json      # Mapping Elasticsearch
├── .env.example                  # Template des variables d'environnement
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
- [FRED API](https://fred.stlouisfed.org/docs/api/fred/) — Documentation API prix du pétrole
- [GDELT API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) — Documentation API événements géopolitiques
