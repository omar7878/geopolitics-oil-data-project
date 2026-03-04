"""
main_pipeline_dag.py
====================
DAGs Airflow — Pipeline Geopolitics × Oil Data Lake.

Deux DAGs :

1. ``oil_geopolitics_init``   (déclenchement **manuel**, une seule fois)
   Ingestion historique la plus ancienne possible, puis construction
   complète des couches Silver → Gold → Elasticsearch → Kibana.
   ⚠️  yfinance est limité à ~60 jours d'historique en granularité 15 min ;
   le start_date est donc calculé dynamiquement.

2. ``oil_geopolitics_daily``  (planifié **tous les jours à 08:00 UTC**)
   Exécution incrémentale quotidienne (Bronze → Silver → Gold → ES).

Pipeline :

  ┌─────────────────┐   ┌─────────────────┐
  │  ingest_gdelt   │   │ ingest_yfinance │
  └────────┬────────┘   └────────┬────────┘
           │                     │
           ▼                     ▼
  ┌─────────────────┐   ┌─────────────────┐
  │  clean_gdelt    │   │ clean_yfinance  │
  └────────┬────────┘   └────────┬────────┘
           │                     │
           └──────────┬──────────┘
                      ▼
           ┌─────────────────────┐
           │ compute_stress_index│
           └──────────┬──────────┘
                      ▼
           ┌─────────────────────┐
           │  index_to_elastic   │
           └──────────┬──────────┘
                      ▼
           ┌─────────────────────┐
           │   setup_kibana      │   ← (init uniquement)
           └─────────────────────┘

Variables Airflow utilisées :
  - execution_date ({{ ds }}) : date de traitement (format YYYY-MM-DD)

Interface web Airflow :
  - URL      : http://localhost:8080
  - Username : admin
  - Password : admin
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT DES TÂCHES
# ──────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# Chemin Python dans le conteneur Airflow (src/ monté en /opt/airflow/src)
PYTHON_CMD = "cd /opt/airflow && python"

# Date la plus ancienne souhaitée (début du projet)
HISTORY_START = datetime(2026, 1, 4)

# ──────────────────────────────────────────────
# CALLABLES PYTHON
# ──────────────────────────────────────────────


def _run_backfill_yfinance(**context: dict) -> None:  # type: ignore[type-arg]
    """Backfill yfinance avec gestion dynamique de la limite 60 jours."""
    import sys
    from datetime import timezone

    sys.path.insert(0, "/opt/airflow")
    from src.ingestion.backfill_yfinance import extract_historical_data  # noqa: PLC0415

    # yfinance interdit les données 15 min au-delà de ~60 jours
    earliest_allowed = datetime.now(timezone.utc) - timedelta(days=59)
    start = max(
        HISTORY_START.replace(tzinfo=timezone.utc),
        earliest_allowed,
    )
    logger.info("Backfill yfinance depuis %s (limite 60 jours)", start.date())
    extract_historical_data(start_date=start)


def _run_index_to_elastic(**context: dict) -> None:  # type: ignore[type-arg]
    """Callable Airflow pour l'étape d'indexation."""
    import sys

    sys.path.insert(0, "/opt/airflow")
    from src.indexing.load_to_elastic import main  # noqa: PLC0415

    logger.info("Lancement de l'indexation Elasticsearch — date : %s", context["ds"])
    main()


def _check_silver_layer_exists(**context: dict) -> bool:  # type: ignore[type-arg]
    """Vérifie que les fichiers Silver (formatted/) existent dans S3.

    Retourne True si les deux parquets sont présents, False sinon.
    """
    import boto3

    endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="eu-west-1",
    )

    required_prefixes = [
        "formatted/yahoofinance/wti.parquet/",
        "formatted/gdelt/events.parquet/",
    ]
    for prefix in required_prefixes:
        resp = s3.list_objects_v2(Bucket="datalake", Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) == 0:
            logger.warning("Fichier Silver manquant : s3://datalake/%s", prefix)
            return False

    logger.info("Couche Silver OK — les fichiers formatted/ sont présents.")
    return True


def _ensure_silver_layer(**context: dict) -> None:  # type: ignore[type-arg]
    """Vérifie la couche Silver ; reconstruit via history si absente.

    Appelé au début du DAG daily pour gérer le cas où LocalStack
    a perdu ses données (redémarrage conteneur, reboot PC, etc.).
    Si les fichiers formatted/ existent déjà, ne fait rien.
    """
    import subprocess
    import sys

    if _check_silver_layer_exists(**context):
        return

    logger.warning("═" * 60)
    logger.warning("RECONSTRUCTION AUTO — Couche Silver absente (S3 vidé ?)")
    logger.warning("═" * 60)

    # Étape 1 : Backfill Bronze (ingestion historique)
    logger.info("→ Backfill GDELT…")
    subprocess.check_call(
        [sys.executable, "-m", "src.ingestion.backfill_gdelt"],
        cwd="/opt/airflow",
    )

    logger.info("→ Backfill yfinance…")
    _run_backfill_yfinance(**context)

    # Étape 2 : Nettoyage Silver (history)
    logger.info("→ Clean GDELT history…")
    subprocess.check_call(
        [sys.executable, "-m", "src.transformation.clean_gdelt", "--mode", "history"],
        cwd="/opt/airflow",
    )

    logger.info("→ Clean yfinance history…")
    subprocess.check_call(
        [sys.executable, "-m", "src.transformation.clean_yfinance", "--mode", "history"],
        cwd="/opt/airflow",
    )

    # Étape 3 : Gold (stress index complet)
    logger.info("→ Compute stress index history…")
    subprocess.check_call(
        [sys.executable, "-m", "src.combination.compute_stress_index", "--mode", "history"],
        cwd="/opt/airflow",
    )

    # Étape 4 : Indexation Elasticsearch
    logger.info("→ Indexation Elasticsearch…")
    _run_index_to_elastic(**context)

    logger.info("Reconstruction terminée ✅")


# ══════════════════════════════════════════════
# DAG 1 — INIT (déclenchement manuel, une fois)
# ══════════════════════════════════════════════

with DAG(
    dag_id="oil_geopolitics_init",
    description="Initialisation complète : backfill historique → Silver → Gold → ES → Kibana",
    schedule=None,                    # Déclenchement manuel uniquement
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["datalake", "oil", "geopolitics", "init"],
) as dag_init:

    # ── Bronze : Backfill GDELT (depuis Jan 4 2026) ──────────────────
    backfill_gdelt = BashOperator(
        task_id="backfill_gdelt",
        bash_command=f"{PYTHON_CMD} -m src.ingestion.backfill_gdelt",
    )

    # ── Bronze : Backfill yfinance (max 60 jours glissants) ──────────
    backfill_yfinance = PythonOperator(
        task_id="backfill_yfinance",
        python_callable=_run_backfill_yfinance,
    )

    # ── Silver : Nettoyage historique GDELT ───────────────────────────
    clean_gdelt_hist = BashOperator(
        task_id="clean_gdelt_history",
        bash_command=f"{PYTHON_CMD} -m src.transformation.clean_gdelt --mode history",
    )

    # ── Silver : Nettoyage historique yfinance ────────────────────────
    clean_yfinance_hist = BashOperator(
        task_id="clean_yfinance_history",
        bash_command=f"{PYTHON_CMD} -m src.transformation.clean_yfinance --mode history",
    )

    # ── Gold : Stress Index complet ──────────────────────────────────
    compute_stress_hist = BashOperator(
        task_id="compute_stress_index_history",
        bash_command=f"{PYTHON_CMD} -m src.combination.compute_stress_index --mode history",
    )

    # ── Indexation Elasticsearch ─────────────────────────────────────
    index_elastic_init = PythonOperator(
        task_id="index_to_elastic",
        python_callable=_run_index_to_elastic,
    )

    # ── Dashboard Kibana ─────────────────────────────────────────────
    setup_kibana = BashOperator(
        task_id="setup_kibana",
        bash_command=f"{PYTHON_CMD} -m src.visualization.setup_kibana",
    )

    # ── Dépendances ──────────────────────────────────────────────────
    backfill_gdelt   >> clean_gdelt_hist
    backfill_yfinance >> clean_yfinance_hist
    [clean_gdelt_hist, clean_yfinance_hist] >> compute_stress_hist
    compute_stress_hist >> index_elastic_init >> setup_kibana


# ══════════════════════════════════════════════
# DAG 2 — DAILY (tous les jours à 08:00 UTC)
# ══════════════════════════════════════════════

with DAG(
    dag_id="oil_geopolitics_daily",
    description="Pipeline incrémental quotidien : extract → clean → stress index → ES",
    schedule="0 8 * * *",             # Tous les jours à 08:00 UTC
    start_date=datetime(2026, 1, 4),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["datalake", "oil", "geopolitics", "daily"],
) as dag_daily:

    # ── Pré-vérification : les fichiers Silver existent-ils ? ────────
    # Si non (ex: redémarrage LocalStack, reboot PC), reconstruction
    # automatique complète (backfill → clean → stress → ES).
    # Si oui, cette tâche ne fait rien et le daily continue.
    ensure_silver = PythonOperator(
        task_id="ensure_silver_layer",
        python_callable=_ensure_silver_layer,
    )

    # ── Bronze : Extraction GDELT du jour ────────────────────────────
    extract_gdelt = BashOperator(
        task_id="extract_gdelt",
        bash_command=(
            f"{PYTHON_CMD} -m src.ingestion.batch_extract_gdelt "
            "--date {{ ds }}"
        ),
    )

    # ── Bronze : Extraction yfinance du jour ─────────────────────────
    extract_yfinance = BashOperator(
        task_id="extract_yfinance",
        bash_command=(
            f"{PYTHON_CMD} -m src.ingestion.batch_extract_yfinance "
            "--date {{ ds }}"
        ),
    )

    # ── Silver : Nettoyage GDELT (incrémental) ───────────────────────
    clean_gdelt = BashOperator(
        task_id="clean_gdelt",
        bash_command=(
            f"{PYTHON_CMD} -m src.transformation.clean_gdelt "
            "--mode daily --date {{ ds }}"
        ),
    )

    # ── Silver : Nettoyage yfinance (incrémental) ────────────────────
    clean_yfinance = BashOperator(
        task_id="clean_yfinance",
        bash_command=(
            f"{PYTHON_CMD} -m src.transformation.clean_yfinance "
            "--mode daily --date {{ ds }}"
        ),
    )

    # ── Gold : Stress Index du jour ──────────────────────────────────
    compute_stress_index = BashOperator(
        task_id="compute_stress_index",
        bash_command=(
            f"{PYTHON_CMD} -m src.combination.compute_stress_index "
            "--mode daily --date {{ ds }}"
        ),
    )

    # ── Indexation Elasticsearch ─────────────────────────────────────
    index_to_elastic = PythonOperator(
        task_id="index_to_elastic",
        python_callable=_run_index_to_elastic,
    )

    # ── Dépendances ──────────────────────────────────────────────────
    ensure_silver >> [extract_gdelt, extract_yfinance]
    extract_gdelt    >> clean_gdelt
    extract_yfinance >> clean_yfinance
    [clean_gdelt, clean_yfinance] >> compute_stress_index
    compute_stress_index >> index_to_elastic
