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
import os
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

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
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

    # ActionGeo_CountryCode est conservé pour le calcul de actor_countries
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
        .master("local[2]")
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
        .config("spark.driver.memory", "1g")
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        .getOrCreate()
    )


# ──────────────────────────────────────────────
# __________________ SCORE GEO (RAW) __________________
# ──────────────────────────────────────────────


# ── Poids ISO 3 lettres (Actor1CountryCode, Actor2CountryCode) ──
COUNTRY_WEIGHTS_ISO3 = {
    # Classe 3 — Les Maîtres du Robinet (Price Makers & Menaces immédiates)
    "SAU": 6, "RUS": 6, "IRN": 6, "IRQ": 6,

    # Classe 2 — Piliers de l'Offre & Risques de Rupture (Supply Risk)
    "USA": 4, "ARE": 3, "KWT": 3, "CAN": 3, "NOR": 3, "KAZ": 3, "BRA": 3,
    "VEN": 3, "LBY": 3, "NGA": 3, "AGO": 3,

    # Classe 1 — Acheteurs Majeurs & Verrous Logistiques (Demand & Transit)
    "CHN": 2, "IND": 2, "MEX": 2, "EGY": 2, "TUR": 2, "PAN": 2, "YEM": 2,
    "JPN": 2, "KOR": 2, "DEU": 2, "FRA": 2, "GBR": 2,
}

# ── Poids FIPS 2 lettres (ActionGeo_CountryCode) ──────────────
# GDELT utilise FIPS 10-4 pour ActionGeo, pas ISO.
# Mapping : https://en.wikipedia.org/wiki/List_of_FIPS_country_codes
COUNTRY_WEIGHTS_FIPS2 = {
    # Classe 3
    "SA": 6, "RS": 6, "IR": 6, "IZ": 6,

    # Classe 2
    "US": 4, "AE": 3, "KU": 3, "CA": 3, "NO": 3, "KZ": 3, "BR": 3,
    "VE": 3, "LY": 3, "NI": 3, "AO": 3,

    # Classe 1
    "CH": 2, "IN": 2, "MX": 2, "EG": 2, "TU": 2, "PM": 2, "YM": 2,
    "JA": 2, "KS": 2, "GM": 2, "FR": 2, "UK": 2,
}

# ── Conversion FIPS 2 lettres → ISO 3 lettres ─────────────────
# Permet de normaliser actor_countries en ISO3 quel que soit
# le champ source (Actor*CountryCode = ISO3, ActionGeo = FIPS2).
FIPS2_TO_ISO3 = {
    # Classe 3
    "SA": "SAU", "RS": "RUS", "IR": "IRN", "IZ": "IRQ",
    # Classe 2
    "US": "USA", "AE": "ARE", "KU": "KWT", "CA": "CAN", "NO": "NOR",
    "KZ": "KAZ", "BR": "BRA", "VE": "VEN", "LY": "LBY", "NI": "NGA",
    "AO": "AGO",
    # Classe 1
    "CH": "CHN", "IN": "IND", "MX": "MEX", "EG": "EGY", "TU": "TUR",
    "PM": "PAN", "YM": "YEM", "JA": "JPN", "KS": "KOR", "GM": "DEU",
    "FR": "FRA", "UK": "GBR",
}


def _country_class_expr(colname: str) -> F.Column:
    """
    Map pays -> classe (1..3) selon ta taxonomie.
    Tout pays absent => 1.
    Utilise le dictionnaire FIPS 2 lettres pour ActionGeo_CountryCode,
    et ISO 3 lettres pour Actor1/Actor2CountryCode.
    """
    weights = COUNTRY_WEIGHTS_FIPS2 if colname == "ActionGeo_CountryCode" else COUNTRY_WEIGHTS_ISO3
    mapping = F.create_map([F.lit(x) for kv in weights.items() for x in kv])
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

    # actor_countries = ensemble dédupliqué des pays impliqués,
    # normalisés en ISO 3 lettres.
    # Sources : ActionGeo_CountryCode (FIPS2→ISO3), Actor1/Actor2CountryCode
    #           (déjà ISO3, mais on tente le mapping FIPS2 au cas où).
    # Les None/null sont exclus ; les doublons aussi.
    fips2_map = F.create_map([F.lit(x) for kv in FIPS2_TO_ISO3.items() for x in kv])
    action_geo_iso3 = fips2_map.getItem(F.col("ActionGeo_CountryCode"))
    actor1_iso3 = F.coalesce(
        fips2_map.getItem(F.col("Actor1CountryCode")),
        F.col("Actor1CountryCode"),
    )
    actor2_iso3 = F.coalesce(
        fips2_map.getItem(F.col("Actor2CountryCode")),
        F.col("Actor2CountryCode"),
    )

    # array_distinct(array(…)) déduplique ; array_compact enlève les null
    actor_countries = F.array_distinct(
        F.array_compact(F.array(action_geo_iso3, actor1_iso3, actor2_iso3))
    )

    df = (
        df
        .withColumn("geo_I", I.cast(DoubleType()))
        .withColumn("geo_B", B.cast(DoubleType()))
        .withColumn("geo_S", S.cast(DoubleType()))
        .withColumn("geo_score_raw", score_raw.cast(DoubleType()))
        .withColumn("actor_countries", actor_countries)
        .drop("geo_C1", "geo_C2", "geo_C3")
    )

    return df


# ──────────────────────────────────────────────
# CLEANING LOGIC (TA TRANSFO INITIALE ADAPTÉE)
# ──────────────────────────────────────────────

def _clean_dataframe(df: DataFrame, sort: bool = True) -> DataFrame:
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
    if sort:
        df = df.orderBy("DATEADDED")

    return df


def _write_parquet(df: DataFrame, path: str) -> None:
    df.coalesce(4).write.mode("overwrite").parquet(path)
    logger.info("Parquet écrit → %s", path)


# ──────────────────────────────────────────────
# 1. FORMAT HISTORY
# ──────────────────────────────────────────────

def format_history() -> None:
    """
    Traitement par lots de jours pour éviter l'OOM JVM.
    On liste les dossiers de dates via boto3, puis on traite
    BATCH_DAYS jours à la fois et on écrit en mode append.
    """
    import boto3

    BATCH_DAYS = 3  # Nb de jours par batch Spark

    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="eu-west-1",
    )

    # Lister les sous-dossiers de date (ex: raw/gdelt/history/2026-01-04/)
    prefix = "raw/gdelt/history/"
    paginator = s3_client.get_paginator("list_objects_v2")
    date_folders = set()
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"]  # ex: "raw/gdelt/history/2026-01-04/"
            date_folders.add(folder)

    date_folders = sorted(date_folders)
    if not date_folders:
        logger.error("Aucun dossier trouvé dans %s", prefix)
        return

    logger.info("═" * 60)
    logger.info("FORMAT HISTORY — %d jours à traiter (lots de %d)", len(date_folders), BATCH_DAYS)
    logger.info("═" * 60)

    spark = _get_spark()
    first_batch = True

    for i in range(0, len(date_folders), BATCH_DAYS):
        batch = date_folders[i: i + BATCH_DAYS]
        paths = [f"s3a://{BUCKET_NAME}/{f}" for f in batch]
        logger.info("Batch %d/%d — jours %s → %s",
                    i // BATCH_DAYS + 1,
                    (len(date_folders) + BATCH_DAYS - 1) // BATCH_DAYS,
                    batch[0].split("/")[-2],
                    batch[-1].split("/")[-2])

        try:
            df = (
                spark.read
                .option("mergeSchema", "false")
                .parquet(*paths)
            )
        except Exception as e:
            logger.warning("Batch ignoré (pas de fichiers lisibles) : %s", e)
            continue

        df = _clean_dataframe(df, sort=False)
        write_mode = "overwrite" if first_batch else "append"
        df.write.mode(write_mode).parquet(FORMATTED_PATH)
        logger.info("  → batch écrit (%s)", write_mode)
        first_batch = False
        spark.catalog.clearCache()

    # ── Passe finale : dédoublonnage global + tri ──────────────
    logger.info("Passe finale — dédoublonnage global + tri")
    spark2 = _get_spark()
    df_final = spark2.read.parquet(FORMATTED_PATH)
    df_final = df_final.dropDuplicates(["GlobalEventID"]).orderBy("DATEADDED")
    df_final.write.mode("overwrite").parquet(FORMATTED_PATH)
    n = spark2.read.parquet(FORMATTED_PATH).count()
    logger.info("  → %d lignes finales", n)
    spark2.stop()
    logger.info("Format history terminé ✅")


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

    # Nettoyer UNIQUEMENT les nouvelles données (les existantes sont déjà clean).
    # Re-cleaner df_existing casserait DATEADDED/Day (le cast("string") suivi de
    # to_timestamp(yyyyMMddHHmmss) ne matche pas un timestamp déjà parsé).
    df_new_clean = _clean_dataframe(df_new, sort=False)

    # Union + dédoublonnage (un même GlobalEventID peut arriver dans daily ET history)
    df_merged = df_existing.unionByName(df_new_clean, allowMissingColumns=False)
    df_merged = df_merged.dropDuplicates(["GlobalEventID"]).orderBy("DATEADDED")

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