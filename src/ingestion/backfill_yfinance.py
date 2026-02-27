"""
batch_extract_yahoofinance.py
=============================
Étape 1 — Ingestion historique du prix du pétrole brut WTI.

Source  : Yahoo Finance via yfinance
          Ticker  : CL=F (WTI Crude Oil Futures)
          Docs    : https://finance.yahoo.com/quote/CL=F

Flux    : Yahoo Finance → DataFrame Parquet → s3://datalake/raw/yahoofinance/history/wti_history_init.parquet

Appelé une seule fois pour initialiser l'historique depuis le 5 janvier 2026.
"""

import io
import logging
import os
from datetime import datetime, timezone

import boto3
import yfinance as yf

# ──────────────────────────────────────────────
# CONFIGURATION GLOBALE
# ──────────────────────────────────────────────

# Ticker Yahoo Finance du pétrole brut WTI (Futures)
TICKER = "CL=F"

# Granularité des données (15 minutes)
INTERVAL = "15m"

# S3 (LocalStack)
S3_ENDPOINT = "http://localhost:4566"
BUCKET_NAME = "datalake"
S3_HISTORY_PREFIX = "raw/yahoofinance/history"

os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)

# ──────────────────────────────────────────────
# CONFIGURATION DU LOGGING
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

def extract_historical_data(start_date: datetime = datetime(2026, 2, 19, 0, 0, tzinfo=timezone.utc)) -> None:
    """
    Télécharge l'historique du pétrole WTI depuis Yahoo Finance,
    nettoie le DataFrame et le sauvegarde en Parquet.
    """
    date_debut = start_date.strftime("%Y-%m-%d")
    date_fin = datetime.today().strftime("%Y-%m-%d")

    logger.info("═" * 60)
    logger.info("Démarrage de l'ingestion historique WTI (Yahoo Finance)")
    logger.info("Ticker   : %s", TICKER)
    logger.info("Période  : %s → %s", date_debut, date_fin)
    logger.info("Intervalle : %s", INTERVAL)
    logger.info("═" * 60)

    # ── 1. Téléchargement via yfinance ────────────────────────
    logger.info("Téléchargement des données en cours...")
    df = yf.download(
        tickers=TICKER,
        start=date_debut,
        end=date_fin,
        interval=INTERVAL,
        auto_adjust=True,   # Ajustement automatique des prix (splits, dividendes)
        progress=False,     # Désactive la barre de progression
    )

    # ── 2. Vérification des données récupérées ────────────────
    if df.empty:
        logger.error("Aucune donnée récupérée. Vérifiez le ticker ou la plage de dates.")
        return

    logger.info("Données récupérées : %d lignes × %d colonnes", *df.shape)

    # ── 3. Nettoyage basique ──────────────────────────────────
    # Réinitialise l'index pour transformer l'index temporel en colonne "Datetime"
    df.columns = df.columns.droplevel(1) if df.columns.nlevels > 1 else df.columns
    df = df.reset_index()
    df = df.rename(columns={"index": "Datetime", "Datetime": "Datetime"})

    logger.info("Colonnes disponibles : %s", list(df.columns))
    logger.info("Plage temporelle     : %s → %s", df["Datetime"].min(), df["Datetime"].max())

    # ── 4. Sauvegarde en Parquet sur S3 (un fichier par jour) ──
    df["_date"] = df["Datetime"].dt.strftime("%Y-%m-%d")

    for date_folder, group in df.groupby("_date"):
        group = group.drop(columns=["_date"])
        parquet_buffer = io.BytesIO()
        group.to_parquet(
            parquet_buffer, engine="pyarrow", index=False,
            coerce_timestamps="us", allow_truncated_timestamps=True,
        )
        parquet_buffer.seek(0)

        s3_key = f"{S3_HISTORY_PREFIX}/{date_folder}/wti_history.parquet"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
        )
        logger.info("✅ Uploadé → s3://%s/%s (%d lignes)", BUCKET_NAME, s3_key, len(group))

    logger.info("Taille totale : %d lignes", len(df))
    logger.info("Ingestion terminée.")


# ──────────────────────────────────────────────
# POINT D'ENTRÉE
# ──────────────────────────────────────────────

if __name__ == "__main__":
    extract_historical_data()
