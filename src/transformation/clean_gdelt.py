"""
clean_gdelt.py
==============
Étape 2 — Formatting (couche Silver) des données brutes GDELT avec PySpark.

Piloté par Airflow via argparse :
  --mode history           Initialisation unique du fichier formaté.
  --mode daily --date D    Mise à jour incrémentale d'un jour précis.

Deux modes :
  1. format_history()
     → Lit TOUT raw/gdelt/history/ (données du backfill initial)
     → Nettoie, filtre, dédoublonne et crée le parquet initial
       formatted/gdelt/events.parquet.
     → À lancer UNE SEULE FOIS au démarrage du projet.

  2. format_daily(target_date)
     → Lit UNIQUEMENT raw/gdelt/daily/{target_date}/ (1 seul jour)
     → Fusionne avec le parquet formaté existant (union)
     → Dédoublonne, filtre et écrase le fichier formaté.
     → Appelé QUOTIDIENNEMENT par Airflow (--date {{ ds }}).
     → Scalable : ne relit pas tout daily/, juste le dossier du jour.

Nettoyage appliqué :
  - DATEADDED → TimestampType (UTC)
  - Day → DateType
  - Cast numériques (GoldsteinScale, AvgTone, coordonnées…)
  - Suppression doublons sur GlobalEventID
  - Filtre : événements géopolitiques majeurs uniquement
    (NumArticles >= 4, |GoldsteinScale| >= 5, QuadClass >= 2, EventRootCode ciblés)
  - Filtre temporel : Day >= 2026-01-01
  - Tri chronologique sur DATEADDED

Usage :
  poetry run python -m src.transformation.clean_gdelt --mode history
  poetry run python -m src.transformation.clean_gdelt --mode daily --date 2026-02-27
"""

import logging
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    IntegerType,
    DoubleType,
)


# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────

S3_ENDPOINT = "http://localhost:4566"
BUCKET_NAME = "datalake"

RAW_HISTORY_PATH = f"s3a://{BUCKET_NAME}/raw/gdelt/history/"
RAW_DAILY_PATH = f"s3a://{BUCKET_NAME}/raw/gdelt/daily/"
FORMATTED_PATH = f"s3a://{BUCKET_NAME}/formatted/gdelt/events.parquet"


# ──────────────────────────────────────────────
# FILTRES — ÉVÉNEMENTS GÉOPOLITIQUES MAJEURS
# ──────────────────────────────────────────────

# Seuils de qualité et d'intensité afin de réduire le bruit
FILTER_MIN_ARTICLES = 4        # Consensus médiatique minimal
FILTER_MIN_GOLDSTEIN = 5.0     # Choc diplomatique/matériel fort (valeur absolue)
FILTER_MIN_QUADCLASS = 2       # Cooperation (2) Conflits Verbaux (3) ou Matériels (4) uniquement

# EventRootCode conservés :
#   Tensions & Menaces
#   10 Demand, 11 Disapprove, 12 Reject, 13 Threaten,
#   15 Exhibit force posture, 17 Coerce
#
#   Chocs & Actions directes
#   06 Material cooperation (ex: accords OPEP)
#   08 Yield (ex: levée d'embargo)
#   14 Protest (ex: grèves pétrolières)
#   16 Reduce relations (ex: sanctions)
#   18 Assault, 19 Fight, 20 Mass violence

MAJOR_EVENT_CODES = [6, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]


# Colonnes conservées (schéma GDELT parquet)
KEEP_COLS = [
    "GlobalEventID",
    "Day",
    "DATEADDED",

    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1Type1Code",

    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2Type1Code",

    "EventCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "IsRootEvent",

    "ActionGeo_CountryCode",
    "ActionGeo_Lat",
    "ActionGeo_Long",

    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
]


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
    return (
        SparkSession.builder
        .appName("clean_gdelt")
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
# CLEANING LOGIC (TA TRANSFO INITIALE ADAPTÉE)
# ──────────────────────────────────────────────

def _clean_dataframe(df: DataFrame) -> DataFrame:
    """
    Applique le nettoyage GDELT :

    - Sélection colonnes utiles
    - Cast types
    - Parse dates
    - Suppression doublons sur GlobalEventID
    - Filtre événements majeurs (NumArticles, GoldsteinScale, QuadClass, EventRootCode)
    - Tri chronologique
    """


    df = df.select(*KEEP_COLS)

    df = (
        df
        # ── Parsing robuste des dates (cast explicite en string) ──
        .withColumn(
            "DATEADDED",
            F.to_timestamp(
                F.col("DATEADDED").cast("string"),
                "yyyyMMddHHmmss"
            )
        )
        .withColumn(
            "Day",
            F.to_date(
                F.col("Day").cast("string"),
                "yyyyMMdd"
            )
        )

        # ── Cast numériques ───────────────────────────────────────
        .withColumn("GlobalEventID", F.col("GlobalEventID").cast(LongType()))
        .withColumn("EventCode", F.col("EventCode").cast(IntegerType()))
        .withColumn("EventRootCode", F.col("EventRootCode").cast(IntegerType()))
        .withColumn("QuadClass", F.col("QuadClass").cast(IntegerType()))
        .withColumn("GoldsteinScale", F.col("GoldsteinScale").cast(DoubleType()))
        .withColumn("IsRootEvent", F.col("IsRootEvent").cast(IntegerType()))
        .withColumn("NumMentions", F.col("NumMentions").cast(IntegerType()))
        .withColumn("NumSources", F.col("NumSources").cast(IntegerType()))
        .withColumn("NumArticles", F.col("NumArticles").cast(IntegerType()))
        .withColumn("AvgTone", F.col("AvgTone").cast(DoubleType()))
        .withColumn("ActionGeo_Lat", F.col("ActionGeo_Lat").cast(DoubleType()))
        .withColumn("ActionGeo_Long", F.col("ActionGeo_Long").cast(DoubleType()))
    )

    # ── Dédoublonnage sur GlobalEventID ───────────────────────────────────
    # Note : filtrer avant dédoublonner réduirait le volume traité,
    # mais Catalyst (Predicates Pushdown) réordonne automatiquement.
    # L'ordre ici reste correct et lisible.
    df = df.dropDuplicates(["GlobalEventID"])

    # ── Filtre — Événements géopolitiques majeurs ─────────────
    df = df.filter(
        (F.col("Day") >= F.lit("2026-01-01").cast("date")) &
        (F.col("NumArticles") >= FILTER_MIN_ARTICLES) &
        (F.abs(F.col("GoldsteinScale")) >= FILTER_MIN_GOLDSTEIN) &
        (F.col("QuadClass") >= FILTER_MIN_QUADCLASS) &
        F.col("EventRootCode").isin(MAJOR_EVENT_CODES)
    )

    # ── Tri chronologique ─────────────────────────────────────
    df = df.orderBy("DATEADDED")

    return df


def _write_parquet(df: DataFrame, path: str) -> None:
    df = df.cache()
    count = df.count()
    df.coalesce(1).write.mode("overwrite").parquet(path)
    logger.info("Parquet écrit → %s (%d lignes)", path, count)
    df.unpersist()


# ──────────────────────────────────────────────
# 1. FORMAT HISTORY
# ──────────────────────────────────────────────

def format_history() -> None:
    spark = _get_spark()

    logger.info("═" * 60)
    logger.info("FORMAT HISTORY — Création fichier initial")
    logger.info("═" * 60)

    try:
        df = spark.read.option("recursiveFileLookup", "true").parquet(RAW_HISTORY_PATH)
    except Exception as e:
        logger.error("Aucun fichier dans %s : %s", RAW_HISTORY_PATH, e)
        spark.stop()
        return

    df = _clean_dataframe(df)

    dt_min, dt_max = df.agg(F.min("DATEADDED"), F.max("DATEADDED")).collect()[0]
    logger.info("Plage temporelle : %s → %s", dt_min, dt_max)

    _write_parquet(df, FORMATTED_PATH)
    logger.info("Format history terminé ✅")

    spark.stop()


# ──────────────────────────────────────────────
# 2. FORMAT DAILY (INCRÉMENTAL)
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

    # Charger parquet existant
    try:
        df_existing = spark.read.parquet(FORMATTED_PATH)
    except Exception:
        logger.warning("Aucun fichier formaté existant. Lance d'abord format_history().")
        spark.stop()
        return

    # Charger uniquement les fichiers du jour cible (pas de recursiveFileLookup)
    target_path = f"{RAW_DAILY_PATH}{target_date}/"
    try:
        df_new = spark.read.parquet(target_path)
    except Exception:
        logger.info("Aucun fichier dans %s — rien à faire.", target_path)
        spark.stop()
        return

    df_merged = df_existing.unionByName(df_new, allowMissingColumns=True)
    df_merged = _clean_dataframe(df_merged)

    dt_min, dt_max = df_merged.agg(F.min("DATEADDED"), F.max("DATEADDED")).collect()[0]
    logger.info("Plage temporelle : %s → %s", dt_min, dt_max)

    _write_parquet(df_merged, FORMATTED_PATH)
    logger.info("Format daily terminé ✅")

    spark.stop()


# ──────────────────────────────────────────────
# POINT D'ENTRÉE
# ──────────────────────────────────────────────

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Formatting GDELT (PySpark)")
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
