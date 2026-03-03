"""
clean_yfinance.py
=================
Étape 2 — Formatting (couche Silver) des données brutes Yahoo Finance (WTI) avec PySpark.

Piloté par Airflow via argparse :
  --mode history           Initialisation unique du fichier formaté.
  --mode daily --date D    Mise à jour incrémentale d'un jour précis.

Deux modes :
  1. format_history()
     → Lit TOUT raw/yahoofinance/history/ (données du backfill initial)
     → Nettoie, dédoublonne, enrichit et crée le parquet initial
       formatted/yahoofinance/wti.parquet.
     → À lancer UNE SEULE FOIS au démarrage du projet.

  2. format_daily(target_date)
     → Lit UNIQUEMENT raw/yahoofinance/daily/{target_date}/ (1 seul jour)
     → Fusionne avec le parquet formaté existant (union)
     → Dédoublonne, enrichit et écrase le fichier formaté.
     → Appelé QUOTIDIENNEMENT par Airflow (--date {{ ds }}).
     → Scalable : ne relit pas tout daily/, juste le dossier du jour.

Nettoyage appliqué :
  - Datetime → UTC (TimestampType)
  - Open, High, Low, Close → DoubleType
  - Volume → LongTypes
  - Suppression des doublons sur Datetime
  - Tri chronologique

Enrichissement (Silver) :
  - Volatility_Range   = High - Low (amplitude de la bougie)
  - Variation_Pct      = (Close - Prev_Close) / Prev_Close * 100
                         (calculé via Window.lag — nécessite l'union avec
                         l'historique existant pour que la 1ère ligne du jour
                         ait une référence t-1 correcte)

Usage :
  poetry run python -m src.transformation.clean_yfinance --mode history
  Exemple: poetry run python -m src.transformation.clean_yfinance --mode daily --date 2026-02-27
"""

import logging
import argparse
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, TimestampType
from pyspark.sql.window import Window

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
BUCKET_NAME = "datalake"

RAW_HISTORY_PATH = f"s3a://{BUCKET_NAME}/raw/yahoofinance/history/"
RAW_DAILY_PATH = f"s3a://{BUCKET_NAME}/raw/yahoofinance/daily/"
FORMATTED_PATH = f"s3a://{BUCKET_NAME}/formatted/yahoofinance/wti.parquet"

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
# SPARK SESSION
# ──────────────────────────────────────────────

def _get_spark() -> SparkSession:
    """Crée une SparkSession configurée pour LocalStack S3."""
    return (
        SparkSession.builder
        .appName("clean_yfinance")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Perf S3A / LocalStack ──────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864")   # 64 MB
        .config("spark.sql.parquet.mergeSchema", "false")           # évite la lecture footer / fichier
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def _clean_dataframe(df: DataFrame) -> DataFrame:
    """
    Applique le nettoyage et l'enrichissement standard sur Yahoo Finance.
    """

    # ── Cast des types ────────────────────────────────────────
    df = (
        df
        .withColumn("Datetime", F.to_utc_timestamp(F.col("Datetime").cast(TimestampType()), "UTC"))
        .withColumn("Open", F.col("Open").cast(DoubleType()))
        .withColumn("High", F.col("High").cast(DoubleType()))
        .withColumn("Low", F.col("Low").cast(DoubleType()))
        .withColumn("Close", F.col("Close").cast(DoubleType()))
        .withColumn("Volume", F.col("Volume").cast(LongType()))
    )

    # ── Dédoublonnage sur Datetime (garde la dernière occurrence) ──
    window_dedup = Window.partitionBy("Datetime").orderBy(F.monotonically_increasing_id())
    before = df.count()
    df = (
        df
        .withColumn("_row_num", F.row_number().over(window_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    after = df.count()
    if before != after:
        logger.info("Doublons supprimés : %d", before - after)

    # ── ENRICHISSEMENT SILVER (Nouvelles Colonnes) ────────────
    
    # 1. Volatilité (Amplitude de la bougie)
    df = df.withColumn("Volatility_Range", F.round(F.col("High") - F.col("Low"), 4))

    # 2. Calcul de la Variation en % par rapport à la bougie précédente
    # On définit une fenêtre triée par le temps pour récupérer le (t-1)
    window_lag = Window.orderBy("Datetime")
    
    df = (
        df
        .withColumn("Prev_Close", F.lag("Close", 1).over(window_lag))
        .withColumn(
            "Variation_Pct",
            F.when(
                F.col("Prev_Close").isNotNull(),
                F.round(((F.col("Close") - F.col("Prev_Close")) / F.col("Prev_Close")) * 100, 4)
            ).otherwise(F.lit(None).cast(DoubleType()))  # null pour la première ligne (pas de référence t-1)
        )
        .drop("Prev_Close")
    )

    # ── Tri chronologique final ───────────────────────────────
    df = df.orderBy("Datetime")

    return df

def _write_parquet(df: DataFrame, path: str) -> None:
    """Écrit un DataFrame Spark en un seul fichier Parquet sur S3."""
    count = df.count()
    df.coalesce(1).write.mode("overwrite").parquet(path)
    logger.info("Parquet écrit → %s  (%d lignes)", path, count)


# ──────────────────────────────────────────────
# 1. FORMAT HISTORY (init)
# ──────────────────────────────────────────────

def format_history() -> None:
    # ... Reste du code inchangé ...
    spark = _get_spark()
    logger.info("═" * 60)
    logger.info("FORMAT HISTORY — Création du fichier formaté initial")
    logger.info("═" * 60)

    try:
        df = (
            spark.read
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "false")
            .parquet(RAW_HISTORY_PATH)
        )
    except Exception as e:
        logger.error("Aucun fichier Parquet trouvé dans %s : %s", RAW_HISTORY_PATH, e)
        spark.stop()
        return

    logger.info("Total brut : %d lignes", df.count())
    df = _clean_dataframe(df)
    logger.info("Après nettoyage : %d lignes", df.count())

    dt_min = df.agg(F.min("Datetime")).collect()[0][0]
    dt_max = df.agg(F.max("Datetime")).collect()[0][0]
    logger.info("Plage temporelle : %s → %s", dt_min, dt_max)

    _write_parquet(df, FORMATTED_PATH)
    logger.info("Format history terminé")
    spark.stop()


# ──────────────────────────────────────────────
# 2. FORMAT DAILY (incrémental)
# ──────────────────────────────────────────────

def format_daily(target_date: str) -> None:
    """
    Mise à jour incrémentale : lit uniquement le dossier du jour cible,
    fusionne avec le parquet formaté existant, nettoie et écrase.

    Args:
        target_date: Date au format YYYY-MM-DD (injectée par Airflow via {{ ds }}).
    """
    spark = _get_spark()
    logger.info("═" * 60)
    logger.info("FORMAT DAILY — Mise à jour incrémentale")
    logger.info("Date cible : %s", target_date)
    logger.info("═" * 60)

    try:
        df_existing = spark.read.parquet(FORMATTED_PATH)
        logger.info("Fichier formaté existant : %d lignes", df_existing.count())
    except Exception:
        logger.warning("Aucun fichier formaté existant (%s). Lancez d'abord format_history().", FORMATTED_PATH)
        spark.stop()
        return

    # Lecture ciblée : uniquement le dossier du jour (pas de recursiveFileLookup)
    target_path = f"{RAW_DAILY_PATH}{target_date}/"
    try:
        df_new = spark.read.parquet(target_path)
    except Exception:
        logger.info("Aucun fichier dans %s — rien à faire.", target_path)
        spark.stop()
        return

    logger.info("Nouvelles lignes brutes : %d", df_new.count())

    # Nettoyer UNIQUEMENT les nouvelles données (les existantes sont déjà clean).
    df_new_clean = _clean_dataframe(df_new)

    # Union + dédoublonnage
    df_merged = df_existing.unionByName(df_new_clean, allowMissingColumns=False)

    # Dédoublonnage sur Datetime (garde la première occurrence)
    window_dedup = Window.partitionBy("Datetime").orderBy(F.monotonically_increasing_id())
    df_merged = (
        df_merged
        .withColumn("_row_num", F.row_number().over(window_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        .orderBy("Datetime")
    )

    dt_min = df_merged.agg(F.min("Datetime")).collect()[0][0]
    dt_max = df_merged.agg(F.max("Datetime")).collect()[0][0]
    logger.info("Plage temporelle : %s → %s", dt_min, dt_max)

    _write_parquet(df_merged, FORMATTED_PATH)
    logger.info("Format daily terminé ✅")
    spark.stop()


if __name__ == "__main__":
    

    parser = argparse.ArgumentParser(description="Formatting Yahoo Finance WTI (PySpark)")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["history", "daily"],
        help="Mode : 'history' pour l'init, 'daily' pour l'incrémental.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date cible au format YYYY-MM-DD (obligatoire en mode daily). Airflow passe {{ ds }}.",
    )
    args = parser.parse_args()

    if args.mode == "daily":
        if not args.date:
            parser.error("--date est obligatoire en mode daily.")
        format_daily(args.date)
    else:
        format_history()