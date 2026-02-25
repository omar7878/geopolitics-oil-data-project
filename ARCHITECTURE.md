# Architecture du projet

```
geopolitics-oil-data-project/
│
├── dags/                                   # ⏱️ ORCHESTRATION (Lu par Airflow)
│   └── main_pipeline_dag.py                # Le chef d'orchestre qui lance les scripts au bon moment.
│
├── src/                                    # 🧠 CODE SOURCE (Scripts métier)
│   ├── __init__.py
│   │
│   ├── ingestion/                          # Étape 1 : Récupération des données brutes
│   │   ├── __init__.py
│   │   ├── batch_extract_yahoofinance.py   # Batch : Prix du pétrole WTI (Yahoo Finance) → S3 raw/yahoofinance/
│   │   ├── batch_extract_gdelt.py          # Batch : Événements géopolitiques (API GDELT) → S3 raw/gdelt/
│   │   ├── kafka_producer_gdelt.py         # ⏸️ PHASE 2 — Streaming GDELT → Kafka
│   │   └── kafka_consumer_s3.py            # ⏸️ PHASE 2 — Kafka → S3 raw/gdelt/
│   │
│   ├── transformation/                     # Étape 2 : Nettoyage et Formatage (Spark)
│   │   ├── __init__.py
│   │   ├── clean_gdelt.py                  # S3 raw/gdelt/ (JSON) → Parquet → S3 formatted/gdelt/
│   │   └── clean_yahoofinance.py            # S3 raw/yahoofinance/ (JSON) → Parquet → S3 formatted/yahoofinance/
│   │
│   ├── combination/                        # Étape 3 : Création de Valeur (Spark)
│   │   ├── __init__.py
│   │   └── compute_stress_index.py         # Jointure GDELT + FRED → Stress Score + Corrélation → S3 combined/
│   │
│   └── indexing/                           # Étape 4 : Chargement vers Elastic
│       ├── __init__.py
│       └── load_to_elastic.py              # S3 combined/ (Parquet) → Elasticsearch
│
├── notebooks/                              # 🔬 EXPLORATION & VISUALISATION (Développement)
│   ├── 01_explore_raw_s3.ipynb             # Visualiser les JSON bruts sur LocalStack S3
│   ├── 02_explore_formatted_spark.ipynb    # Explorer les Parquet après nettoyage Spark
│   ├── 03_explore_combined_stress_index.ipynb  # Stress Score + corrélation glissante
│   └── 04_explore_elasticsearch.ipynb      # Requêter et visualiser Elasticsearch
│
├── tests/                                  # 🧪 TESTS UNITAIRES (Pytest)
│   ├── __init__.py
│   ├── test_batch_extract_fred.py
│   ├── test_batch_extract_gdelt.py
│   ├── test_clean_fred.py
│   ├── test_clean_gdelt.py
│   ├── test_compute_stress_index.py
│   └── test_load_to_elastic.py
│
├── infrastructure/                         # 🐳 DOCKER & SERVICES
│   ├── docker-compose.yml                  # Lance Airflow, LocalStack, Elastic et Kibana.
│   └── localstack_init.sh                  # Crée automatiquement les buckets S3 au démarrage.
│
├── config/                                 # ⚙️ CONFIGURATION
│   └── elastic_mapping.json                # Schéma des données pour Kibana (types, dates...).
│
├── .env                                    # 🔒 SECRETS (IGNORÉ PAR GIT)
│                                           # FRED_API_KEY et variables de connexion.
│
├── .gitignore                              # 🚫 EXCLUSIONS GIT
│                                           # .env, __pycache__, volume_s3, data/...
│
├── pyproject.toml                          # 📦 DÉPENDANCES (Poetry)
│                                           # requests, boto3, pyspark, elasticsearch, pandas, s3fs
│
├── ARCHITECTURE.md                         # 🗺️ CE FICHIER
│
└── README.md                               # 📖 DOCUMENTATION
```

---

## Structure du Data Lake S3 (LocalStack)

```
s3://datalake/
├── raw/                    ← Données brutes telles que reçues des APIs (JSON)
│   ├── fred/
│   │   └── YYYY-MM-DD/
│   │       └── wti_price.json
│   └── gdelt/
│       └── YYYY-MM-DD/
│           └── events.json
│
├── formatted/              ← Données nettoyées, dates UTC, types corrects (Parquet)
│   ├── fred/
│   └── gdelt/
│
└── combined/               ← Résultat final : jointure + Stress Score (Parquet)
    └── stress_index/
```

---

## Services Docker

| Service | Port | Rôle |
|---|---|---|
| LocalStack (S3) | 4566 | Faux Amazon S3 local |
| Elasticsearch | 9200 | Moteur d'indexation |
| Kibana | 5601 | Dashboard de visualisation |
| Airflow | 8080 | Orchestration du pipeline |
| PostgreSQL | interne | Base de données d'Airflow |
| Kafka + Zookeeper | ⏸️ commentés | PHASE 2 — Streaming temps réel |
