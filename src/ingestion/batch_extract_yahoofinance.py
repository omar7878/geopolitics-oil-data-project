"""
batch_extract_yahoofinance.py
=============================
Étape 1 — Ingestion historique du prix du pétrole brut WTI.

Source  : Yahoo Finance via yfinance
          Ticker  : CL=F (WTI Crude Oil Futures)
          Docs    : https://finance.yahoo.com/quote/CL=F

Flux    : Yahoo Finance → DataFrame Parquet → raw/yahoofinance/history/wti_history_init.parquet

Appelé une seule fois pour initialiser l'historique depuis le 5 janvier 2026.
"""

import logging
import os
from datetime import datetime

import yfinance as yf

# ──────────────────────────────────────────────
# CONFIGURATION GLOBALE
# ──────────────────────────────────────────────

# Ticker Yahoo Finance du pétrole brut WTI (Futures)
TICKER = "CL=F"

# Date de début de l'historique (fixe)
DATE_DEBUT = "2026-01-05"

# Date de fin calculée dynamiquement
DATE_FIN = datetime.today().strftime("%Y-%m-%d")

# Granularité des données (15 minutes)
INTERVAL = "15m"

# Chemin de sortie du fichier Parquet
CHEMIN_SORTIE = os.path.join(
    os.path.dirname(__file__),   # src/ingestion/
    "..", "..", "raw", "yahoofinance", "history",
    "wti_history_init.parquet"
)
CHEMIN_SORTIE = os.path.abspath(CHEMIN_SORTIE)

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

def extract_historical_data() -> None:
    """
    Télécharge l'historique du pétrole WTI depuis Yahoo Finance,
    nettoie le DataFrame et le sauvegarde en Parquet.
    """

    logger.info("═" * 60)
    logger.info("Démarrage de l'ingestion historique WTI (Yahoo Finance)")
    logger.info("Ticker   : %s", TICKER)
    logger.info("Période  : %s → %s", DATE_DEBUT, DATE_FIN)
    logger.info("Intervalle : %s", INTERVAL)
    logger.info("═" * 60)

    # ── 1. Téléchargement via yfinance ────────────────────────
    logger.info("Téléchargement des données en cours...")
    df = yf.download(
        tickers=TICKER,
        start=DATE_DEBUT,
        end=DATE_FIN,
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

    # ── 4. Création du dossier cible si nécessaire ────────────
    dossier_sortie = os.path.dirname(CHEMIN_SORTIE)
    os.makedirs(dossier_sortie, exist_ok=True)
    logger.info("Dossier cible : %s", dossier_sortie)

    # ── 5. Sauvegarde en Parquet ──────────────────────────────
    df.to_parquet(CHEMIN_SORTIE, engine="pyarrow", index=False)
    logger.info("Fichier Parquet écrit avec succès : %s", CHEMIN_SORTIE)
    logger.info("Taille du fichier : %.2f Ko", os.path.getsize(CHEMIN_SORTIE) / 1024)
    logger.info("Ingestion terminée.")


# ──────────────────────────────────────────────
# POINT D'ENTRÉE
# ──────────────────────────────────────────────

if __name__ == "__main__":
    extract_historical_data()
