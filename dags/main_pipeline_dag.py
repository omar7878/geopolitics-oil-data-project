"""
main_pipeline_dag.py
====================
DAG Airflow — Pipeline complet Geopolitics × Oil Data Lake.

Orchestre les 5 étapes de la chaîne de valeur (Bronze → Silver → Gold → Usage → Kibana) :

  ┌─────────────────┐   ┌─────────────────┐
  │ extract_gdelt   │   │ extract_yfinance │
  └────────┬────────┘   └────────┬────────┘
           │                     │
           ▼                     ▼
  ┌─────────────────┐   ┌─────────────────┐
  │  clean_gdelt    │   │  clean_yfinance  │
  └────────┬────────┘   └────────┬────────┘
           │                     │
           └──────────┬──────────┘
                      ▼
           ┌─────────────────────┐
           │  compute_stress_idx  │
           └──────────┬──────────┘
                      ▼
           ┌─────────────────────┐
           │  xgboost_predict    │   ← exécute le notebook 03bis via nbconvert
           └──────────┬──────────┘
                      ▼
           ┌─────────────────────┐
           │  index_to_elastic   │   ← src/indexing/load_to_elastic.py
           └─────────────────────┘

Planification :
  @daily à 06:00 UTC (marchés WTI ouverts → données de la veille disponibles)

Variables Airflow utilisées :
  - execution_date ({{ ds }}) : date de traitement (format YYYY-MM-DD)
"""

from __future__ import annotations

import logging
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

# ──────────────────────────────────────────────
# CALLABLES PYTHON
# ──────────────────────────────────────────────


def _run_index_to_elastic(**context: dict) -> None:  # type: ignore[type-arg]
    """
    Callable Airflow pour l'étape d'indexation.
    Importe et appelle main() de load_to_elastic.
    """
    import sys
    import os

    sys.path.insert(0, "/opt/airflow")
    from src.indexing.load_to_elastic import main  # noqa: PLC0415

    logger.info("Lancement de l'indexation Elasticsearch — date : %s", context["ds"])
    main()


# ──────────────────────────────────────────────
# DÉFINITION DU DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="oil_geopolitics_pipeline",
    description="Pipeline complet : GDELT + Yahoo Finance → Stress Index → XGBoost → Elasticsearch",
    schedule_interval="0 6 * * 1-5",   # Lundi–Vendredi à 06:00 UTC (jours de marché)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["datalake", "oil", "geopolitics", "elasticsearch"],
) as dag:

    # ── Étape 1 — Bronze : Ingestion GDELT ────────────────────────────────
    extract_gdelt = BashOperator(
        task_id="extract_gdelt",
        bash_command=(
            f"{PYTHON_CMD} -m src.ingestion.batch_extract_gdelt "
            "--date {{ ds }}"
        ),
    )

    # ── Étape 1 — Bronze : Ingestion Yahoo Finance (WTI) ──────────────────
    extract_yfinance = BashOperator(
        task_id="extract_yfinance",
        bash_command=(
            f"{PYTHON_CMD} -m src.ingestion.batch_extract_yfinance "
            "--date {{ ds }}"
        ),
    )

    # ── Étape 2 — Silver : Nettoyage GDELT ────────────────────────────────
    clean_gdelt = BashOperator(
        task_id="clean_gdelt",
        bash_command=(
            f"{PYTHON_CMD} -m src.transformation.clean_gdelt "
            "--mode daily --date {{ ds }}"
        ),
    )

    # ── Étape 2 — Silver : Nettoyage Yahoo Finance ────────────────────────
    clean_yfinance = BashOperator(
        task_id="clean_yfinance",
        bash_command=(
            f"{PYTHON_CMD} -m src.transformation.clean_yfinance "
            "--mode daily --date {{ ds }}"
        ),
    )

    # ── Étape 3 — Gold : Calcul du Stress Index géopolitique ──────────────
    compute_stress_index = BashOperator(
        task_id="compute_stress_index",
        bash_command=(
            f"{PYTHON_CMD} -m src.combination.compute_stress_index "
            "--mode daily --date {{ ds }}"
        ),
    )

    # ── Étape 4 — Usage : Prédictions XGBoost (exécution du notebook) ─────
    # Le notebook 03bis_xgboost_prediction.ipynb lit la table Gold et
    # écrit les prédictions dans s3://datalake/usage/oil_predictions/.
    xgboost_predict = BashOperator(
        task_id="xgboost_predict",
        bash_command=(
            "jupyter nbconvert --to notebook --execute "
            "--inplace /opt/airflow/notebooks/03bis_xgboost_prediction.ipynb "
            "--ExecutePreprocessor.timeout=600"
        ),
    )

    # ── Étape 5 — Indexation Elasticsearch ────────────────────────────────
    index_to_elastic = PythonOperator(
        task_id="index_to_elastic",
        python_callable=_run_index_to_elastic,
    )

    # ──────────────────────────────────────────────
    # DÉPENDANCES DU PIPELINE
    # ──────────────────────────────────────────────

    # Ingestion en parallèle
    [extract_gdelt, extract_yfinance]

    # Nettoyage après ingestion (chacun dépend de sa source)
    extract_gdelt    >> clean_gdelt
    extract_yfinance >> clean_yfinance

    # Stress Index dépend des deux Silver
    [clean_gdelt, clean_yfinance] >> compute_stress_index

    # XGBoost après Gold
    compute_stress_index >> xgboost_predict

    # Indexation Elasticsearch après prédictions
    xgboost_predict >> index_to_elastic
