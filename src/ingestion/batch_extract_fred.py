"""
batch_extract_fred.py
=====================
Étape 1 — Ingestion batch du prix du pétrole brut WTI.

Source  : API FRED (Federal Reserve Economic Data)
          Série   : DCOILWTICO (WTI Crude Oil Price, Daily)
          Docs    : https://fred.stlouisfed.org/series/DCOILWTICO

Flux    : FRED API → JSON → S3 raw/fred/YYYY-MM-DD/wti_price.json

Appelé par : Airflow (1x/jour, voir main_pipeline_dag.py)
"""

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import requests
from botocore.config import Config
from dotenv import load_dotenv

# ──────────────────────────────────────────────
# Configuration du logger
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("batch_extract_fred")

# ──────────────────────────────────────────────
# Chargement des variables d'environnement
# ──────────────────────────────────────────────
load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")

S3_BUCKET = "datalake"
S3_PREFIX = "raw/fred"
FRED_SERIES_ID = "DCOILWTICO"  # WTI Crude Oil Price (Daily)
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


# ──────────────────────────────────────────────
# Client S3 (LocalStack)
# ──────────────────────────────────────────────
def get_s3_client():
    """Crée et retourne un client boto3 pointant vers LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
        config=Config(signature_version="s3v4"),
    )


# ──────────────────────────────────────────────
# Extraction depuis l'API FRED
# ──────────────────────────────────────────────
def fetch_wti_prices(start_date: str, end_date: str) -> dict:
    """
    Interroge l'API FRED pour récupérer les prix WTI entre deux dates.

    Args:
        start_date: Date de début au format YYYY-MM-DD
        end_date:   Date de fin   au format YYYY-MM-DD

    Returns:
        Dictionnaire JSON brut retourné par l'API FRED.

    Raises:
        ValueError: Si la clé API est manquante.
        requests.HTTPError: Si l'API retourne une erreur HTTP.
    """
    if not FRED_API_KEY:
        raise ValueError(
            "FRED_API_KEY manquante. Ajoutez-la dans le fichier .env"
        )

    params = {
        "series_id": FRED_SERIES_ID,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
        "frequency": "d",          # Fréquence journalière
        "aggregation_method": "avg",
    }

    logger.info(f"Appel API FRED : {FRED_SERIES_ID} de {start_date} à {end_date}")
    response = requests.get(FRED_BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    nb_observations = len(data.get("observations", []))
    logger.info(f"{nb_observations} observations reçues de FRED.")
    return data


# ──────────────────────────────────────────────
# Sauvegarde sur S3 (LocalStack)
# ──────────────────────────────────────────────
def save_to_s3(data: dict, date_partition: str) -> str:
    """
    Sauvegarde le JSON brut sur LocalStack S3.

    Structure : s3://datalake/raw/fred/YYYY-MM-DD/wti_price.json

    Args:
        data:            Données JSON à sauvegarder.
        date_partition:  Date au format YYYY-MM-DD (partition S3).

    Returns:
        Chemin S3 complet du fichier créé.
    """
    s3_key = f"{S3_PREFIX}/{date_partition}/wti_price.json"
    payload = json.dumps(data, indent=2, ensure_ascii=False)

    s3 = get_s3_client()
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )

    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    logger.info(f"Données sauvegardées sur : {s3_path}")
    return s3_path


# ──────────────────────────────────────────────
# Point d'entrée principal
# ──────────────────────────────────────────────
def run(execution_date: str = None):
    """
    Orchestre l'extraction et la sauvegarde des données FRED.

    Par défaut, récupère les 90 derniers jours de données pour
    avoir de l'historique dès le premier lancement.

    Args:
        execution_date: Date cible au format YYYY-MM-DD.
                        Si None, utilise la date d'aujourd'hui.
    """
    end_date = execution_date or datetime.utcnow().strftime("%Y-%m-%d")
    # On remonte 90 jours en arrière pour avoir un historique suffisant
    start_date = (
        datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=90)
    ).strftime("%Y-%m-%d")

    logger.info(f"=== Démarrage extraction FRED | {start_date} → {end_date} ===")

    try:
        data = fetch_wti_prices(start_date, end_date)
        s3_path = save_to_s3(data, date_partition=end_date)
        logger.info(f"=== Extraction FRED terminée avec succès → {s3_path} ===")
        return s3_path
    except Exception as e:
        logger.error(f"Échec de l'extraction FRED : {e}")
        raise


if __name__ == "__main__":
    run()
