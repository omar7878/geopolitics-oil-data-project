"""
batch_extract_yfinance.py
=========================
Ingestion journalière idempotente du prix du pétrole brut WTI (15 min).

Source  : Yahoo Finance via yfinance — Ticker CL=F
Flux    : Yahoo Finance → Parquet → s3://datalake/raw/yahoofinance/daily/YYYY-MM-DD/wti_daily.parquet

Usage:
    python -m src.ingestion.batch_extract_yfinance --date 2026-02-27
    # Airflow passe {{ ds }} automatiquement via le paramètre --date
"""

import argparse
import io
import logging
import os
from datetime import date, timedelta

import boto3
import yfinance as yf

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────

TICKER = "CL=F"
INTERVAL = "15m"

S3_ENDPOINT = "http://localhost:4566"
BUCKET_NAME = "datalake"

os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# FONCTION PRINCIPALE
# ──────────────────────────────────────────────

def extract_daily_data(execution_date: date) -> None:
    """
    Télécharge les données WTI d'une journée spécifique (intervalle 15 min)
    et les sauvegarde en Parquet sur S3 dans raw/yahoofinance/daily/{date}/.

    Idempotent : relancer avec la même date écrase le fichier existant.
    """
    date_start = execution_date.strftime("%Y-%m-%d")
    date_end   = (execution_date + timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("═" * 60)
    logger.info("INGESTION DAILY WTI (Yahoo Finance)")
    logger.info("Ticker     : %s", TICKER)
    logger.info("Journée    : %s (UTC)", date_start)
    logger.info("Intervalle : %s", INTERVAL)
    logger.info("═" * 60)

    # ── 1. Téléchargement via yfinance ────────────────────────
    logger.info("Téléchargement des données en cours...")
    df = yf.download(
        tickers=TICKER,
        start=date_start,
        end=date_end,
        interval=INTERVAL,
        auto_adjust=True,
        progress=False,
    )

    # ── 2. Vérification ──────────────────────────────────────
    if df.empty:
        logger.warning("Aucune donnée pour %s (marché fermé ?).", date_start)
        return

    logger.info("Données récupérées : %d lignes × %d colonnes", *df.shape)

    # ── 3. Nettoyage basique ─────────────────────────────────
    df.columns = df.columns.droplevel(1) if df.columns.nlevels > 1 else df.columns
    df = df.reset_index()
    df = df.rename(columns={"index": "Datetime", "Datetime": "Datetime"})

    logger.info("Colonnes : %s", list(df.columns))
    logger.info("Plage    : %s → %s", df["Datetime"].min(), df["Datetime"].max())

    # ── 4. Upload Parquet sur S3 ─────────────────────────────
    s3_key = f"raw/yahoofinance/daily/{date_start}/wti_daily.parquet"

    parquet_buffer = io.BytesIO()
    df.to_parquet(
        parquet_buffer, engine="pyarrow", index=False,
        coerce_timestamps="us", allow_truncated_timestamps=True,
    )
    parquet_buffer.seek(0)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=parquet_buffer.getvalue(),
    )
    logger.info("✅ Uploadé → s3://%s/%s", BUCKET_NAME, s3_key)
    logger.info("Taille : %.2f Ko", len(parquet_buffer.getvalue()) / 1024)
    logger.info("Ingestion daily terminée.")


# ──────────────────────────────────────────────
# POINT D'ENTRÉE
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestion journalière WTI (Yahoo Finance)")
    parser.add_argument(
        "--date",
        required=True,
        type=lambda s: date.fromisoformat(s),
        help="Date à ingérer au format YYYY-MM-DD (ex: 2026-02-27). Airflow passe {{ ds }}.",
    )
    args = parser.parse_args()
    extract_daily_data(execution_date=args.date)
