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
from pyspark.sql.window import Window


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

FILTER_MIN_ARTICLES = 4        # Consensus médiatique minimal
FILTER_MIN_GOLDSTEIN = 5.0     # Choc diplomatique/matériel fort (valeur absolue)
FILTER_MIN_QUADCLASS = 2       # Cooperation (2) Conflits Verbaux (3) ou Matériels (4) uniquement

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

    # ActionGeo_CountryCode est conservé pour le calcul du dominant_country
    # (il est déjà présent plus bas, on s'assure de ne pas le dupliquer)

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
# __________________ SCORE GEO (RAW) __________________
# ──────────────────────────────────────────────

def _country_class_expr(colname: str) -> F.Column:
    """
    Map pays -> classe (1..4) selon ta taxonomie.
    Tout pays absent => 1.
    """
    country_weights = {
        # Classe 4 — Game Changers
        "USA": 4, "SAU": 4, "RUS": 4, "CHN": 4,

        # Classe 3 — Piliers Offre & Hubs / Verrous
        "IRN": 3, "IRQ": 3, "KWT": 3, "ARE": 3, "QAT": 3,
        "CAN": 3, "BRA": 3, "NOR": 3, "MEX": 3, "KAZ": 3,
        "EGY": 3, "TUR": 3, "PAN": 3, "YEM": 3,

        # Classe 2 — Influence indirecte / instables / diplomatie
        "VEN": 2, "NGA": 2, "LBY": 2, "AGO": 2,
        "FRA": 2, "GBR": 2, "DEU": 2, "JPN": 2,
        "IND": 2, "ISR": 2, "KOR": 2,
    }

    mapping = F.create_map([F.lit(x) for kv in country_weights.items() for x in kv])
    return F.coalesce(mapping.getItem(F.col(colname)), F.lit(1)).cast(IntegerType())


def _clip01(x: F.Column) -> F.Column:
    return F.greatest(F.lit(0.0), F.least(F.lit(1.0), x))


def _add_geo_scores(df: DataFrame) -> DataFrame:
    """
    Ajoute 4 colonnes :
      - geo_I         : importance géo (Leader+Bonus), I ∈ [1,7]
      - geo_B         : buzz = log1p(NumArticles)
      - geo_S         : sévérité bornée (Goldstein + Tone)
      - geo_score_raw : score brut combiné
    """

    # Classes pays (1..4)
    c1 = _country_class_expr("Actor1CountryCode")
    c2 = _country_class_expr("Actor2CountryCode")
    c3 = _country_class_expr("ActionGeo_CountryCode")

    df = (
        df
        .withColumn("geo_C1", c1)
        .withColumn("geo_C2", c2)
        .withColumn("geo_C3", c3)
    )

    # I = max + 0.5 * ( (c1-1)+(c2-1)+(c3-1) - (max-1) )
    maxc = F.greatest(F.col("geo_C1"), F.col("geo_C2"), F.col("geo_C3"))
    I = (
        maxc.cast(DoubleType()) +
        F.lit(0.5) * (
            (F.col("geo_C1") + F.col("geo_C2") + F.col("geo_C3") - maxc) - F.lit(2)
        ).cast(DoubleType())
    )

    # B = log(1+NumArticles)
    B = F.log1p(F.col("NumArticles").cast(DoubleType()))

    # g = clip(-Goldstein/10, 0, 1), t = clip(-AvgTone/100, 0, 1)
    g = _clip01(-F.col("GoldsteinScale") / F.lit(10.0))
    t = _clip01(-F.col("AvgTone") / F.lit(100.0))

    # S = 1 + 1.5g + 1.0t   (borné dans [1, 3.5])
    S = F.lit(1.0) + F.lit(1.5) * g + F.lit(1.0) * t

    # Score raw = I^2 * B * S
    score_raw = (I * I) * B * S

    # dominant_country = le pays avec la classe la plus élevée parmi les 3
    # En cas d'égalité : Actor1 > Actor2 > ActionGeo (priorité à l'initiateur)
    dominant_country = (
        F.when(
            (F.col("geo_C1") >= F.col("geo_C2")) & (F.col("geo_C1") >= F.col("geo_C3")),
            F.col("Actor1CountryCode")
        ).when(
            F.col("geo_C2") >= F.col("geo_C3"),
            F.col("Actor2CountryCode")
        ).otherwise(
            F.col("ActionGeo_CountryCode")
        )
    )

    df = (
        df
        .withColumn("geo_I", I.cast(DoubleType()))
        .withColumn("geo_B", B.cast(DoubleType()))
        .withColumn("geo_S", S.cast(DoubleType()))
        .withColumn("geo_score_raw", score_raw.cast(DoubleType()))
        .withColumn("dominant_country", dominant_country)
        .drop("geo_C1", "geo_C2", "geo_C3")
    )

    return df


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
    df = df.dropDuplicates(["GlobalEventID"])

    # ── Filtre — Événements géopolitiques majeurs ─────────────
    df = df.filter(
        (F.col("Day") >= F.lit("2026-01-01").cast("date")) &
        (F.col("NumArticles") >= FILTER_MIN_ARTICLES) &
        (F.abs(F.col("GoldsteinScale")) >= FILTER_MIN_GOLDSTEIN) &
        (F.col("QuadClass") >= FILTER_MIN_QUADCLASS) &
        F.col("EventRootCode").isin(MAJOR_EVENT_CODES)
    )

    # ─────────────────────────────────────────────────────────────
    # __________________ AJOUT SCORES GEO (4 COLONNES) _____________
    # ─────────────────────────────────────────────────────────────
    df = _add_geo_scores(df)

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
        df = (
            spark.read
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "false")
            .parquet(RAW_HISTORY_PATH)
        )
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