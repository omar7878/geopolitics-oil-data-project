# ============================================================
# batch_extract_gdelt.py
# Étape 1 : Ingestion batch journalière des événements GDELT
#
# Rôle : Génère les 96 timestamps 15-min d'une journée UTC donnée,
#        télécharge chaque fichier export GDELT, convertit en Parquet
#        et uploade sur S3 dans raw/gdelt/daily/{date}/.
#
# Appelé par : Airflow (1 fois par jour, Airflow passe {{ ds }})
# Output      : s3://datalake/raw/gdelt/daily/YYYY-MM-DD/*.parquet
#
# Usage:
#   python -m src.ingestion.batch_extract_gdelt --date 2026-02-27
# ============================================================

import argparse
import io
import logging
import os
import zipfile
from datetime import date, datetime, timedelta, timezone

import boto3
import pandas as pd
import requests

# ==============================================================================
# CONFIGURATION
# ==============================================================================

S3_ENDPOINT  = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
BUCKET_NAME  = "datalake"
GDELT_BASE_URL = "http://data.gdeltproject.org/gdeltv2"

os.environ["AWS_ACCESS_KEY_ID"]     = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"]    = "eu-west-1"

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)

# Colonnes GDELT v2 Events (58 colonnes)
GDELT_COLUMNS = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName",
    "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
    "Actor2Geo_Long", "Actor2Geo_FeatureID", "ActionGeo_Type",
    "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long",
    "ActionGeo_FeatureID", "DATEADDED", "SOURCEURL",
]

# ==============================================================================
# LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ==============================================================================
# HELPERS
# ==============================================================================

def _build_timestamps(execution_date: date) -> list[datetime]:
    """Génère les 96 timestamps UTC de 00:00:00 à 23:45:00 par pas de 15 min."""
    start = datetime(execution_date.year, execution_date.month, execution_date.day,
                     tzinfo=timezone.utc)
    return [start + timedelta(minutes=15 * i) for i in range(96)]


def _process_timestamp(ts: datetime, date_str: str) -> bool:
    """
    Télécharge, convertit et uploade le fichier GDELT pour un timestamp donné.
    Retourne True si succès, False si 404 (fichier absent côté GDELT).
    """
    ts_str  = ts.strftime("%Y%m%d%H%M%S")   # format GDELT : 14 chiffres
    zip_url = f"{GDELT_BASE_URL}/{ts_str}.export.CSV.zip"

    try:
        resp = requests.get(zip_url, timeout=30)
        if resp.status_code == 404:
            logger.warning("404 — fichier absent (normal) : %s", zip_url)
            return False
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Erreur HTTP pour %s : %s", zip_url, exc)
        return False

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        for filename in z.namelist():
            with z.open(filename) as csv_file:
                df = pd.read_csv(
                    csv_file,
                    sep="\t",
                    header=None,
                    names=GDELT_COLUMNS,
                    dtype=str,
                )

            parquet_name   = filename.replace(".CSV", ".parquet").replace(".csv", ".parquet")
            parquet_buffer = io.BytesIO()
            df.to_parquet(
                parquet_buffer, index=False, engine="pyarrow",
                coerce_timestamps="us", allow_truncated_timestamps=True,
            )
            parquet_buffer.seek(0)

            s3_key = f"raw/gdelt/daily/{date_str}/{parquet_name}"
            s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=parquet_buffer.getvalue())
            logger.info("Uploadé → s3://%s/%s (%d lignes)", BUCKET_NAME, s3_key, len(df))

    return True


# ==============================================================================
# FONCTION PRINCIPALE
# ==============================================================================

def run_gdelt_ingestion(execution_date: date) -> None:
    """
    Ingestion idempotente de tous les fichiers GDELT 15-min d'une journée UTC.
    Les fichiers absents (404) sont ignorés avec un warning.
    """
    date_str   = execution_date.strftime("%Y-%m-%d")
    timestamps = _build_timestamps(execution_date)

    logger.info("═" * 60)
    logger.info("INGESTION DAILY GDELT")
    logger.info("Journée   : %s (UTC)", date_str)
    logger.info("Slots     : %d timestamps × 15 min", len(timestamps))
    logger.info("═" * 60)

    success = 0
    missing = 0

    for ts in timestamps:
        ok = _process_timestamp(ts, date_str)
        if ok:
            success += 1
        else:
            missing += 1

    logger.info("═" * 60)
    logger.info("Terminé — %d uploadés, %d absents (404)", success, missing)
    logger.info("═" * 60)


# ==============================================================================
# POINT D'ENTRÉE
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestion journalière GDELT")
    parser.add_argument(
        "--date",
        required=True,
        type=lambda s: date.fromisoformat(s),
        help="Date à ingérer au format YYYY-MM-DD (ex: 2026-02-27). Airflow passe {{ ds }}.",
    )
    args = parser.parse_args()
    run_gdelt_ingestion(execution_date=args.date)