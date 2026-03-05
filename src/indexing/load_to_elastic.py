"""
load_to_elastic.py
==================
Étape 4 — Indexation de la couche Gold (Stress Index) dans Elasticsearch.

Lit le Parquet Gold depuis s3://datalake/combined/stress_index/
(produit par compute_stress_index.py) et indexe toutes les colonnes
dans l'index Elasticsearch `oil-market-analysis` pour exposition Kibana.

Pipeline :
  1. Lecture S3 (boto3 + pyarrow) — combined/stress_index/*.snappy.parquet
  2. Normalisation du timestamp Datetime → UTC ISO-8601 (indispensable Kibana)
  3. Création de l'index Elasticsearch avec mapping explicite (idempotent)
  4. Ingestion bulk via elasticsearch.helpers.bulk

Schéma Parquet Gold (20 colonnes) :
  Datetime, Open, High, Low, Close, Volume,
  Volatility_Range, Variation_Pct,
  geo_I_smoothed, geo_B_smoothed, geo_S_smoothed, geo_score_raw_smoothed,
  geo_I_sum, geo_B_sum, geo_S_sum, geo_score_raw_sum,
  total_event_count, gap_duration_15m, period_actor_country, score_pct_7d

Usage :
  poetry run python -m src.indexing.load_to_elastic

Airflow :
  Appelable via PythonOperator(python_callable=main) dans main_pipeline_dag.py.

Lit le Parquet final depuis s3://datalake/usage/oil_predictions/
et indexe les résultats dans l'index Elasticsearch `oil-market-analysis`
pour exposition Kibana.

Pipeline :
  1. Lecture S3 (boto3 + pyarrow) — usage/oil_predictions/
  2. Normalisation du timestamp en UTC (indispensable pour Kibana)
  3. Création de l'index Elasticsearch avec mapping explicite (idempotent)
  4. Ingestion bulk via elasticsearch.helpers.bulk

Schéma Parquet attendu (colonnes) :
  - timestamp        : datetime du point de marché (15 min)
  - real_price       : prix Close réel WTI (USD)
  - predicted_price  : prédiction XGBoost M1 ou M2 (USD)
  - OSI_score        : Oil Stress Index — percentile géopolitique 7 jours glissants

Usage :
  poetry run python -m src.indexing.load_to_elastic

Airflow :
  Appelable via PythonOperator(python_callable=main) dans main_pipeline_dag.py.
"""

import io
import logging
import math
import os
from typing import Any, Generator

import boto3
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────

# Surchargeables via variables d'environnement Airflow / Docker
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
ES_HOST     = os.getenv("ES_HOST",          "http://localhost:9200")

BUCKET_NAME = "datalake"
S3_PREFIX   = "combined/stress_index/"
ES_INDEX    = "oil-market-analysis"

# Toutes les colonnes de la couche Gold à indexer dans Elasticsearch
COLUMNS = [
    "timestamp",            # Datetime normalisé UTC (renommé depuis Datetime)
    "Open", "High", "Low", "Close", "Volume",
    "Volatility_Range", "Variation_Pct",
    "geo_I_smoothed", "geo_B_smoothed", "geo_S_smoothed", "geo_score_raw_smoothed",
    "geo_I_sum", "geo_B_sum", "geo_S_sum", "geo_score_raw_sum",
    "total_event_count", "gap_duration_15m",
    "period_actor_country",
    "score_pct_7d",
]

# Credentials LocalStack (fictifs mais requis par boto3)
os.environ.setdefault("AWS_ACCESS_KEY_ID",     "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION",    "eu-west-1")

# ──────────────────────────────────────────────
# JOURNALISATION
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# MAPPING ELASTICSEARCH
# ──────────────────────────────────────────────

ES_MAPPING: dict[str, Any] = {
    "mappings": {
        "properties": {
            # ── Temporal ───────────────────────────────────────────────────
            "timestamp":              {"type": "date",    "format": "strict_date_time||epoch_millis"},
            # ── WTI OHLCV ──────────────────────────────────────────────────
            "Open":                   {"type": "float"},
            "High":                   {"type": "float"},
            "Low":                    {"type": "float"},
            "Close":                  {"type": "float"},
            "Volume":                 {"type": "long"},
            # ── Métriques Silver ───────────────────────────────────────────
            "Volatility_Range":       {"type": "float"},
            "Variation_Pct":          {"type": "float"},
            # ── Scores géopolitiques lissés ────────────────────────────────
            "geo_I_smoothed":         {"type": "float"},
            "geo_B_smoothed":         {"type": "float"},
            "geo_S_smoothed":         {"type": "float"},
            "geo_score_raw_smoothed": {"type": "float"},
            # ── Scores bruts (sommes GDELT) ────────────────────────────────
            "geo_I_sum":              {"type": "float"},
            "geo_B_sum":              {"type": "float"},
            "geo_S_sum":              {"type": "float"},
            "geo_score_raw_sum":      {"type": "float"},
            # ── Métriques GDELT ────────────────────────────────────────────
            "total_event_count":      {"type": "float"},
            "gap_duration_15m":       {"type": "integer"},
            "period_actor_country":       {"type": "keyword"},
            # ── Oil Stress Index ───────────────────────────────────────────
            "score_pct_7d":           {"type": "float"},
        }
    },
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,   # env de dev mono-noeud
    },
}

# ──────────────────────────────────────────────
# LECTURE S3
# ──────────────────────────────────────────────


def _read_parquet_from_s3() -> pd.DataFrame:
    """
    Lit tous les fichiers Parquet (*.snappy.parquet) du préfixe
    ``combined/stress_index/`` depuis le bucket LocalStack et retourne
    un DataFrame pandas fusionné.

    Les fichiers sont au format Spark partitionné :
      combined/stress_index/part-00000-<uuid>-c000.snappy.parquet

    Raises
    ------
    FileNotFoundError
        Aucun fichier Parquet trouvé sous le préfixe S3.
    """
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)

    logger.info("Listage des objets dans s3://%s/%s", BUCKET_NAME, S3_PREFIX)
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_PREFIX)
    keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")   # exclut le fichier _SUCCESS Spark
    ]

    if not keys:
        raise FileNotFoundError(
            f"Aucun fichier Parquet trouvé dans s3://{BUCKET_NAME}/{S3_PREFIX}. "
            "Vérifiez que compute_stress_index.py a bien été exécuté avant l'indexation."
        )

    logger.info("%d fichier(s) Parquet détecté(s)", len(keys))
    frames = []
    for key in keys:
        logger.info("  → lecture de %s", key)
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        frames.append(pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow"))

    df = pd.concat(frames, ignore_index=True)
    logger.info("Données chargées : %d lignes | colonnes : %s", len(df), list(df.columns))
    return df


# ──────────────────────────────────────────────
# NORMALISATION UTC
# ──────────────────────────────────────────────


def _normalize_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renomme la colonne ``Datetime`` (nom Spark/Gold) en ``timestamp``
    et la convertit en UTC ISO-8601 (string) — indispensable pour Kibana.

    Raises
    ------
    KeyError
        Si la colonne ``Datetime`` est absente du DataFrame.
    """
    if "Datetime" not in df.columns:
        raise KeyError(
            "Colonne 'Datetime' absente du Parquet Gold. "
            f"Colonnes disponibles : {list(df.columns)}"
        )

    df = df.copy()
    df["timestamp"] = (
        pd.to_datetime(df["Datetime"], utc=True)   # Datetime est naïf UTC → localise
        .dt.strftime("%Y-%m-%dT%H:%M:%SZ")         # format strict ISO-8601 UTC
    )
    df = df.drop(columns=["Datetime"])             # évite le doublon

    logger.info(
        "Timestamps normalisés UTC — plage : %s → %s",
        df["timestamp"].min(),
        df["timestamp"].max(),
    )
    return df


# ──────────────────────────────────────────────
# ELASTICSEARCH — CLIENT ET INDEX
# ──────────────────────────────────────────────


def _get_es_client() -> Elasticsearch:
    """
    Crée et vérifie la connexion au cluster Elasticsearch.

    Raises
    ------
    ConnectionError
        Si le cluster n'est pas joignable.
    """
    es = Elasticsearch(ES_HOST, request_timeout=30)
    if not es.ping():
        raise ConnectionError(
            f"Impossible de joindre Elasticsearch sur {ES_HOST}. "
            "Vérifiez que le conteneur est démarré (docker-compose up elasticsearch)."
        )
    logger.info("Elasticsearch connecté : %s", ES_HOST)
    return es


def _ensure_index(es: Elasticsearch) -> None:
    """
    Crée l'index ``oil-market-analysis`` avec son mapping explicite
    s'il n'existe pas encore. Opération idempotente.
    """
    if es.indices.exists(index=ES_INDEX):
        logger.info("Index '%s' déjà présent — création ignorée.", ES_INDEX)
        return

    es.indices.create(index=ES_INDEX, body=ES_MAPPING)
    logger.info("Index '%s' créé avec le mapping explicite.", ES_INDEX)


# ──────────────────────────────────────────────
# GÉNÉRATION DES DOCUMENTS BULK
# ──────────────────────────────────────────────


def _generate_actions(df: pd.DataFrame) -> Generator[dict[str, Any], None, None]:
    """
    Génère les documents au format attendu par ``elasticsearch.helpers.bulk``.

    Seules les colonnes présentes dans ``COLUMNS`` sont projetées.
    Les NaN / Inf flottants sont convertis en ``None`` (JSON null) car
    Elasticsearch rejette le token ``NaN`` non-standard.
    """
    available = [c for c in COLUMNS if c in df.columns]
    missing   = set(COLUMNS) - set(available)

    if missing:
        logger.warning(
            "Colonnes absentes du Parquet (non indexées) : %s", sorted(missing)
        )

    def _safe(v: Any) -> Any:
        """Convertit float NaN / Inf en None pour sérialisation JSON valide."""
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v

    for row in df[available].itertuples(index=False):
        yield {
            "_index":  ES_INDEX,
            "_source": {col: _safe(getattr(row, col)) for col in available},
        }


# ──────────────────────────────────────────────
# INDEXATION BULK
# ──────────────────────────────────────────────


def _bulk_index(es: Elasticsearch, df: pd.DataFrame) -> None:
    """
    Indexe le DataFrame dans Elasticsearch via ``helpers.bulk``.

    Paramètres
    ----------
    chunk_size : 500 documents par requête → bon équilibre débit / mémoire
    raise_on_error : False pour continuer en cas d'erreur partielle
    """
    logger.info("Démarrage indexation bulk — %d documents à envoyer…", len(df))

    success, errors = bulk(
        es,
        _generate_actions(df),
        chunk_size=500,
        raise_on_error=False,
        stats_only=False,
    )

    logger.info(
        "Indexation terminée : %d succès | %d erreur(s)",
        success,
        len(errors),
    )
    if errors:
        for err in errors[:5]:   # Afficher les 5 premières erreurs uniquement
            logger.error("Erreur bulk : %s", err)
        if len(errors) > 5:
            logger.error("… et %d erreur(s) supplémentaire(s).", len(errors) - 5)


# ──────────────────────────────────────────────
# POINT D'ENTRÉE PRINCIPAL
# ──────────────────────────────────────────────


def main() -> None:
    """
    Pipeline complet d'indexation :

      s3://datalake/usage/oil_predictions/ (Parquet XGBoost)
        → normalisation UTC
        → Elasticsearch index oil-market-analysis
        → Kibana

    Appelable directement (CLI) ou via un ``PythonOperator`` Airflow.
    """
    logger.info("═" * 60)
    logger.info("INDEXATION  oil-market-analysis  (couche Gold → Elasticsearch)")
    logger.info("Source      : s3://%s/%s", BUCKET_NAME, S3_PREFIX)
    logger.info("Index ES    : %s", ES_INDEX)
    logger.info("Endpoint ES : %s", ES_HOST)
    logger.info("═" * 60)

    # 1. Lecture S3 (boto3 + pyarrow)
    df = _read_parquet_from_s3()

    # 2. Normalisation UTC
    df = _normalize_timestamp(df)

    # 3. Connexion Elasticsearch + création de l'index (idempotent)
    es = _get_es_client()
    _ensure_index(es)

    # 4. Ingestion bulk
    _bulk_index(es, df)

    logger.info("═" * 60)
    logger.info("Pipeline d'indexation terminé avec succès.")
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
