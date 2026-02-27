"""
clean_yfinance.py
=================
Étape 2 — Formatting des données brutes Yahoo Finance (WTI) avec PySpark.

Deux modes :
  1. format_history()  → Lit tous les fichiers raw/yahoofinance/history/,
                          les nettoie, les fusionne (sans doublons) et écrit
                          le parquet initial dans formatted/yahoofinance/.
  2. format_daily()    → Lit les nouveaux fichiers raw/yahoofinance/daily/,
                          les nettoie, les fusionne avec le parquet formaté
                          existant (sans doublons) et met à jour le fichier.

Nettoyage appliqué :
  - Datetime → UTC (TimestampType)
  - Open, High, Low, Close → DoubleType
  - Volume → LongType
  - Suppression des doublons sur Datetime
  - Tri chronologique
Enrichissement (Silver) :
  - Volatility_Range (High - Low)
  - Variation_Pct (% d'évolution par rapport à la bougie précédente)
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, TimestampType
from pyspark.sql.window import Window

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────

S3_ENDPOINT = "http://localhost:4566"
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
        df = spark.read.option("recursiveFileLookup", "true").parquet(RAW_HISTORY_PATH)
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

def format_daily() -> None:
    # ... Reste du code inchangé ...
    spark = _get_spark()
    logger.info("═" * 60)
    logger.info("FORMAT DAILY — Mise à jour incrémentale")
    logger.info("═" * 60)

    try:
        df_existing = spark.read.parquet(FORMATTED_PATH)
        logger.info("Fichier formaté existant : %d lignes", df_existing.count())
    except Exception:
        logger.warning("Aucun fichier formaté existant (%s). Lancez d'abord format_history().", FORMATTED_PATH)
        spark.stop()
        return

    try:
        df_new = spark.read.option("recursiveFileLookup", "true").parquet(RAW_DAILY_PATH)
    except Exception:
        logger.info("Aucun nouveau fichier dans %s — rien à faire.", RAW_DAILY_PATH)
        spark.stop()
        return

    logger.info("Nouvelles lignes brutes : %d", df_new.count())

    df_merged = df_existing.unionByName(df_new, allowMissingColumns=True)
    logger.info("Total avant dédoublonnage : %d lignes", df_merged.count())

    df_merged = _clean_dataframe(df_merged)
    logger.info("Total après dédoublonnage : %d lignes", df_merged.count())

    dt_min = df_merged.agg(F.min("Datetime")).collect()[0][0]
    dt_max = df_merged.agg(F.max("Datetime")).collect()[0][0]
    logger.info("Plage temporelle : %s → %s", dt_min, dt_max)

    _write_parquet(df_merged, FORMATTED_PATH)
    logger.info("Format daily terminé ✅")
    spark.stop()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "daily":
        format_daily()
    else:
        format_history()