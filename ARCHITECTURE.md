# Architecture du projet

```
geopolitics-oil-data-project/
│
├── dags/                                   # ORCHESTRATION (Airflow)
│   └── main_pipeline_dag.py                # DAGs init + daily : ordonnancement du pipeline complet.
│
├── src/                                    # CODE SOURCE (Scripts métier)
│   ├── __init__.py
│   │
│   ├── ingestion/                          # Étape 1 : Récupération des données brutes
│   │   ├── __init__.py
│   │   ├── backfill_gdelt.py               # Historique GDELT (Master File List) → S3 raw/gdelt/history/
│   │   ├── backfill_yfinance.py            # Historique WTI (Yahoo Finance, 60j max) → S3 raw/yahoofinance/history/
│   │   ├── batch_extract_gdelt.py          # Daily GDELT (96 fichiers/jour) → S3 raw/gdelt/daily/
│   │   └── batch_extract_yfinance.py       # Daily WTI (1 fichier/jour) → S3 raw/yahoofinance/daily/
│   │
│   ├── transformation/                     # Étape 2 : Nettoyage et formatage (PySpark)
│   │   ├── __init__.py
│   │   ├── clean_gdelt.py                  # raw/gdelt/ → formatted/gdelt/events.parquet
│   │   └── clean_yfinance.py               # raw/yahoofinance/ → formatted/yahoofinance/wti.parquet
│   │
│   ├── combination/                        # Étape 3 : Création de la couche Gold (PySpark)
│   │   ├── __init__.py
│   │   └── compute_stress_index.py         # Jointure WTI × GDELT → combined/stress_index/
│   │
│   ├── indexing/                           # Étape 4 : Chargement vers Elasticsearch
│   │   ├── __init__.py
│   │   └── load_to_elastic.py              # combined/ (Parquet) → index oil-market-analysis
│   │
│   └── visualization/                      # Étape 5 : Dashboard Kibana
│       ├── __init__.py
│       └── setup_kibana.py                 # Data View + visualisations Lens + dashboard
│
├── notebooks/                              # EXPLORATION (Jupyter)
│   ├── 01_explore_raw_s3.ipynb             # Données brutes Parquet sur LocalStack S3
│   ├── 02_explore_formatted_spark.ipynb    # Parquet après nettoyage Spark (couche Silver)
│   ├── 03_explore_combined_stress_index.ipynb  # Stress Index et corrélations
│   ├── 04_grid_search.ipynb                # Recherche d'hyperparamètres
│   └── 05_grid_search.ipynb                # Recherche d'hyperparamètres (suite)
│
├── tests/                                  # TESTS UNITAIRES (pytest)
│   ├── __init__.py
│   ├── test_backfill_gdelt.py
│   ├── test_backfill_yfinance.py
│   ├── test_batch_extract_gdelt.py
│   ├── test_batch_extract_yfinance.py
│   ├── test_clean_gdelt.py
│   ├── test_clean_yfinance.py
│   ├── test_compute_stress_index.py
│   ├── test_load_to_elastic.py
│   └── test_setup_kibana.py
│
├── infrastructure/                         # DOCKER ET SERVICES
│   ├── docker-compose.yml                  # LocalStack, PostgreSQL, Airflow, Elasticsearch, Kibana
│   ├── airflow.Dockerfile                  # Image Airflow avec code embarqué (COPY dags/ + src/)
│   └── localstack_init.sh                  # Création automatique du bucket S3 au démarrage
│
├── config/                                 # CONFIGURATION
│   └── elastic_mapping.json                # Mapping Elasticsearch (types, dates, champs)
│
├── .env                                    # SECRETS (ignoré par Git)
│
├── .gitignore                              # EXCLUSIONS GIT
│
├── pyproject.toml                          # DÉPENDANCES (Poetry)
│
├── ARCHITECTURE.md                         # CE FICHIER
│
└── README.md                               # DOCUMENTATION PRINCIPALE
```

---

## Structure du Data Lake S3 (LocalStack)

```
s3://datalake/
├── raw/                    ← Données brutes telles que reçues des APIs (Parquet)
│   ├── yahoofinance/
│   │   ├── history/        ← Backfill historique (une seule fois)
│   │   │   └── YYYY-MM-DD/wti_history.parquet
│   │   └── daily/          ← Données quotidiennes (15 min)
│   │       └── YYYY-MM-DD/
│   │           └── wti_TIMESTAMP.parquet
│   └── gdelt/
│       ├── history/        ← Backfill historique
│       │   └── YYYY-MM-DD/
│       │       └── events.parquet
│       └── daily/
│           └── YYYY-MM-DD/
│               └── events_TIMESTAMP.parquet
│
├── formatted/              ← Couche Silver : données nettoyées (PySpark)
│   ├── yahoofinance/
│   │   └── wti.parquet/
│   └── gdelt/
│       └── events.parquet/
│
└── combined/               ← Couche Gold : jointure WTI × GDELT + Stress Index
    └── stress_index/
```

---

## Services Docker

| Service         | Port   | Rôle                                       |
|-----------------|--------|---------------------------------------------|
| LocalStack (S3) | 4566   | Stockage objet S3 local                     |
| Elasticsearch   | 9200   | Moteur d'indexation                         |
| Kibana          | 5601   | Dashboard de visualisation                  |
| Airflow         | 8080   | Orchestration du pipeline (admin / admin)   |
| PostgreSQL      | interne| Base de données d'Airflow                   |
