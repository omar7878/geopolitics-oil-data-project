# Geopolitics & Oil Data Project

Analyse de la corrélation entre les événements géopolitiques mondiaux et les prix du pétrole brut WTI, via un Data Lake complet orchestré par Airflow.

**Équipe :** Omar Fekih-Hassen, Alexandre Donnat, Leo Ivars

---

## Sommaire

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture](#architecture)
3. [Prérequis](#prérequis)
4. [Installation](#installation)
5. [Utilisation](#utilisation)
6. [Elasticsearch et Kibana](#elasticsearch-et-kibana)
7. [Tests](#tests)
8. [Structure du projet](#structure-du-projet)

---

## Vue d'ensemble

Ce projet met en place un **Data Lake end-to-end** qui :

1. **Ingère** les prix du pétrole WTI (granularité 15 min) depuis **Yahoo Finance** et les événements géopolitiques depuis l'API **GDELT v2**.
2. **Nettoie et enrichit** les données avec **PySpark** : filtrage des événements majeurs, calcul de scores géopolitiques (Importance, Buzz, Sévérité).
3. **Calcule un indice de stress géopolitique** (Oil Stress Index) combinant les deux sources, avec lissage des périodes de fermeture du marché et percentile glissant 7 jours.
4. **Indexe** les résultats dans **Elasticsearch 8.10** pour visualisation dans **Kibana**.
5. **Déploie un dashboard Kibana** automatiquement via l'API Saved Objects (Data View, visualisations Lens, carte choropleth).

Le pipeline est structuré en **trois couches** (Medallion Architecture) :

| Couche     | Chemin S3                              | Description                                                   |
|------------|----------------------------------------|---------------------------------------------------------------|
| **Raw**    | `raw/gdelt/`, `raw/yahoofinance/`      | Données brutes Parquet, un fichier par tranche de 15 min      |
| **Silver** | `formatted/gdelt/`, `formatted/yahoofinance/` | Nettoyé, filtré, dédoublonné, enrichi (scores géopolitiques) |
| **Gold**   | `combined/stress_index/`               | Table ML-ready : WTI × GDELT jointé, stress lissé, percentile 7j |

---

## Architecture

```
                          ┌─────────────┐
Yahoo Finance (15 min) ──>│             │
                          │  raw/ (S3)  │──> formatted/ (Silver) ──> 
GDELT v2 (15 min)      ──>│  Parquet    │
                          └─────────────┘

combined/ (Gold) ──> Elasticsearch ──> Kibana
```

Le pipeline est orchestré par **deux DAGs Airflow** :

- **`oil_geopolitics_init`** — Déclenchement manuel, une seule fois : backfill historique complet, puis construction Silver, Gold, Elasticsearch et Kibana.
- **`oil_geopolitics_daily`** — Planifié tous les jours à 08:00 UTC : extraction incrémentale, nettoyage, calcul du stress index et indexation.

Le DAG daily intègre une tâche `ensure_silver_layer` qui vérifie l'existence de la couche Silver au démarrage. Si les données ont été perdues (redémarrage de LocalStack par exemple), le pipeline reconstruit automatiquement l'ensemble des couches avant de poursuivre.

Voir [ARCHITECTURE.md](ARCHITECTURE.md) pour l'arborescence complète des fichiers et la structure S3.

---

## Prérequis

| Outil                  | Version minimale | Obligatoire |
|------------------------|------------------|-------------|
| Python                 | 3.10+            | Oui         |
| Poetry                 | 1.8+             | Oui         |
| Docker et Docker Compose | 24+            | Oui         |
| Java (JDK)             | 11+              | Oui (PySpark) |
| Git                    | 2.40+            | Oui         |
| awscli-local           | 0.22+            | Optionnel   |

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

Vérifier que tous les services sont opérationnels (~1 min) :

```bash
docker compose -f infrastructure/docker-compose.yml ps
```

Services disponibles :

| Service         | URL                          | Description                                   |
|-----------------|------------------------------|-----------------------------------------------|
| LocalStack (S3) | http://localhost:4566        | Stockage objet S3 local                       |
| Elasticsearch   | http://localhost:9200        | Moteur d'indexation (single-node, security off)|
| Kibana          | http://localhost:5601        | Dashboard de visualisation                    |
| Airflow         | http://localhost:8080        | Orchestration (identifiants : admin / admin)  |

---

## Utilisation

L'ensemble du pipeline est orchestré par **Airflow**. Aucune commande manuelle n'est nécessaire.

1. Ouvrir http://localhost:8080 (identifiants : admin / admin).
2. Activer et déclencher manuellement le DAG **`oil_geopolitics_init`** (une seule fois) : il effectue le backfill historique complet, puis construit les couches Silver, Gold, indexe dans Elasticsearch et déploie le dashboard Kibana.
3. Activer le DAG **`oil_geopolitics_daily`** pour l'exécution quotidienne automatique (planifié à 08:00 UTC).

> **Remarques :**
> - Tous les scripts sont **idempotents** : relancer un DAG avec la même date écrase les données sans créer de doublons.
> - Le DAG daily intègre une tâche `ensure_silver_layer` qui reconstruit automatiquement les couches si LocalStack a perdu ses données (redémarrage, reboot).

Le DAG daily suit la chaîne suivante :

```
ensure_silver_layer → [extract_gdelt, extract_yfinance]
                          │                    │
                          ▼                    ▼
                    clean_gdelt          clean_yfinance
                          │                    │
                          └────────┬───────────┘
                                   ▼
                        compute_stress_index
                                   ▼
                          index_to_elastic
```

---

## Elasticsearch et Kibana

### Index `oil-market-analysis`

Le script `load_to_elastic` crée l'index avec le mapping défini dans `config/elastic_mapping.json` :

| Champ                  | Type      | Description                                          |
|------------------------|-----------|------------------------------------------------------|
| `timestamp`            | `date`    | Datetime UTC de la bougie WTI (15 min)               |
| `Open`, `High`, `Low`, `Close` | `float` | Prix OHLC du WTI (USD/bbl)                   |
| `Volume`               | `long`    | Volume de trading (contrats)                         |
| `Volatility_Range`     | `float`   | High - Low (amplitude de la bougie)                  |
| `Variation_Pct`        | `float`   | Variation en % par rapport a la bougie precedente    |
| `geo_I_smoothed`       | `float`   | Importance geopolitique lissee                       |
| `geo_B_smoothed`       | `float`   | Buzz mediatique lisse                                |
| `geo_S_smoothed`       | `float`   | Severite lissee                                      |
| `geo_score_raw_smoothed` | `float` | Score brut combine lisse                             |
| `total_event_count`    | `float`   | Nombre d'evenements GDELT cumules                    |
| `gap_duration_15m`     | `integer` | Duree de fermeture du marche (tranches de 15 min)    |
| `period_actor_country` | `keyword` | Pays acteur dominant de la periode                   |
| `score_pct_7d`         | `float`   | Percentile glissant 7 jours (Oil Stress Index)       |

### Dashboard Kibana

Le script `setup_kibana` deploie automatiquement via l'API Saved Objects :

| Panneau                              | Type        | Description                                               |
|--------------------------------------|-------------|-----------------------------------------------------------|
| WTI — Cours (Close)                  | Ligne       | Prix de cloture WTI avec interpolation lineaire           |
| Volume de trading                    | Barres      | Volume des contrats par periode                           |
| Variation WTI vs Score Global        | Double axe  | Superposition variation % et score geopolitique           |
| Scores geopolitiques (smoothed)      | Lignes      | geo_I / geo_B / geo_S superposes                          |
| Top acteurs geopolitiques            | Barres horiz.| Top pays par nombre d'occurrences                        |
| Carte des acteurs                    | Choropleth  | Carte mondiale coloree par frequence des pays acteurs     |

Acces : http://localhost:5601/app/dashboards

---

## Tests

```bash
poetry run pytest tests/ -v
```

103 tests couvrant les 7 modules du pipeline (ingestion, transformation, combinaison, indexation, visualisation).

---

## Structure du projet

```
geopolitics-oil-data-project/
├── dags/                              # DAG Airflow (orchestration quotidienne)
│   └── main_pipeline_dag.py
├── src/
│   ├── ingestion/                     # Extraction des donnees brutes vers S3
│   │   ├── backfill_gdelt.py          #   Historique complet GDELT (Master File List)
│   │   ├── backfill_yfinance.py       #   Historique complet WTI
│   │   ├── batch_extract_gdelt.py     #   Daily GDELT (96 fichiers/jour, --date)
│   │   └── batch_extract_yfinance.py  #   Daily WTI (--date)
│   ├── transformation/                # Nettoyage PySpark vers couche Silver
│   │   ├── clean_gdelt.py             #   Filtre, dedoublonne, scores geo I/B/S
│   │   └── clean_yfinance.py          #   Cast, volatilite, variation %
│   ├── combination/                   # Fusion WTI x GDELT vers couche Gold
│   │   └── compute_stress_index.py    #   Join, lissage fermeture, percentile 7j
│   ├── indexing/                      # Chargement Elasticsearch
│   │   └── load_to_elastic.py         #   Bulk index avec mapping explicite
│   └── visualization/                 # Dashboard Kibana
│       └── setup_kibana.py            #   Data View + Lens + Dashboard
├── tests/                             # Tests unitaires (103 tests, pytest)
├── notebooks/                         # Exploration et prediction (Jupyter)
├── infrastructure/
│   ├── docker-compose.yml             # LocalStack, PostgreSQL, Airflow, ES, Kibana
│   ├── airflow.Dockerfile             # Image Airflow avec code embarque
│   └── localstack_init.sh             # Creation automatique du bucket S3
├── config/
│   └── elastic_mapping.json           # Mapping Elasticsearch (20 champs)
├── pyproject.toml                     # Dependances Poetry
├── ARCHITECTURE.md                    # Architecture detaillee
└── README.md                          # Ce fichier
```

---

## Verifier S3 (optionnel)

```bash
# Avec awslocal
awslocal s3 ls s3://datalake/ --recursive

# Ou avec le CLI AWS standard
aws --endpoint-url=http://localhost:4566 s3 ls s3://datalake/ --recursive
```

---

## Documentation complementaire

- [ARCHITECTURE.md](ARCHITECTURE.md) — Architecture detaillee du Data Lake
- [Yahoo Finance (CL=F)](https://finance.yahoo.com/quote/CL=F) — Prix du petrole brut WTI
- [GDELT v2](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) — Documentation API evenements geopolitiques
- [Elasticsearch 8.10](https://www.elastic.co/guide/en/elasticsearch/reference/8.10/index.html) — Documentation Elasticsearch
- [Kibana Saved Objects API](https://www.elastic.co/guide/en/kibana/8.10/saved-objects-api.html) — API utilisee par `setup_kibana.py`
