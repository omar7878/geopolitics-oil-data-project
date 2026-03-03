"""
compute_stress_index.py
=======================
Étape 3 — Couche Gold (Combined) : Stress Index Géopolitique × WTI.

Fusionne les données Silver (WTI 15-min + GDELT filtré) pour produire
une table ML-ready stockée dans s3://datalake/combined/stress_index/.

Piloté par Airflow via argparse :
  --mode history           Construction complète de la table Gold.
  --mode daily --date D    Mise à jour incrémentale d'un jour précis.

Logique métier :
  Le marché WTI ferme la nuit et le week-end, mais les événements
  GDELT arrivent 24/7. Le stress accumulé pendant la fermeture est
  lissé et reporté sur la PROCHAINE bougie d'ouverture du marché.

Pipeline (5 étapes PySpark) :
  1. Agrégation GDELT par tranche de 15 min
     → somme des scores, actor_countries du pire événement
  2. Full outer join WTI × GDELT sur le timestamp 15 min
     → master_datetime, market_open (0/1)
  3. Forward mapping (fermeture → prochaine réouverture)
     → Window DESC + last(ignoreNulls) → target_open_datetime
  4. Lissage des périodes de fermeture
     → GroupBy target_open_datetime, scores / gap_duration_15m
  5. Jointure finale (inner) + percentile glissant 7 jours
     → Table Gold uniquement market_open, score_pct_7d

Usage :
  poetry run python -m src.combination.compute_stress_index --mode history
  poetry run python -m src.combination.compute_stress_index --mode daily --date 2026-02-27
"""

import argparse
import logging
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
BUCKET_NAME = "datalake"

WTI_FORMATTED_PATH   = f"s3a://{BUCKET_NAME}/formatted/yahoofinance/wti.parquet"
GDELT_FORMATTED_PATH = f"s3a://{BUCKET_NAME}/formatted/gdelt/events.parquet"
GOLD_OUTPUT_PATH     = f"s3a://{BUCKET_NAME}/combined/stress_index"

SEVEN_DAYS_SECONDS = 7 * 24 * 3600   # 604 800 s

SCORE_COLS = ["geo_I", "geo_B", "geo_S", "geo_score_raw"]

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
        .appName("compute_stress_index")
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
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ──────────────────────────────────────────────
# CHARGEMENT DES DONNÉES SILVER
# ──────────────────────────────────────────────

def _load_wti(spark: SparkSession, mode: str, target_date: str | None) -> DataFrame:
    """Charge les données WTI formatées (couche Silver) depuis S3."""
    logger.info("Chargement WTI depuis %s", WTI_FORMATTED_PATH)
    df = spark.read.parquet(WTI_FORMATTED_PATH)

    if mode == "daily" and target_date:
        # En mode daily : 8 jours de contexte (7j pour le percentile + 1j courant)
        dt = datetime.strptime(target_date, "%Y-%m-%d")
        start = (dt - timedelta(days=8)).strftime("%Y-%m-%d")
        df = df.filter(
            (F.col("Datetime") >= F.lit(start))
            & (F.col("Datetime") <= F.lit(target_date + " 23:59:59"))
        )

    count = df.count()
    logger.info("WTI chargé : %d lignes", count)
    return df


def _load_gdelt(spark: SparkSession, mode: str, target_date: str | None) -> DataFrame:
    """Charge les événements GDELT formatés (couche Silver) depuis S3."""
    logger.info("Chargement GDELT depuis %s", GDELT_FORMATTED_PATH)
    df = spark.read.parquet(GDELT_FORMATTED_PATH)

    if mode == "daily" and target_date:
        dt = datetime.strptime(target_date, "%Y-%m-%d")
        start = dt - timedelta(days=8)
        df = df.filter(
            (F.col("DATEADDED") >= F.lit(start.strftime("%Y-%m-%d")))
            & (F.col("DATEADDED") <= F.lit(target_date + " 23:59:59"))
        )

    count = df.count()
    logger.info("GDELT chargé : %d lignes", count)
    return df


# ──────────────────────────────────────────────
# ÉTAPE 1 — AGRÉGATION GDELT PAR TRANCHE 15 MIN
# ──────────────────────────────────────────────

def _aggregate_gdelt_15min(df_gdelt: DataFrame) -> DataFrame:
    """
    GroupBy DATEADDED (déjà arrondi à 15 min côté Silver) :
      - Somme des 4 scores (geo_I, geo_B, geo_S, geo_score_raw)
      - actor_countries = tableau dédupliqué ISO3 de l'événement
        avec le geo_score_raw le plus élevé de la tranche
      - event_count = nombre d'événements dans la tranche
    """
    logger.info("Étape 1 : Agrégation GDELT par tranche de 15 min")

    # ── Agrégation : somme des scores + struct trick pour actor_countries ──
    # actor_countries (calculé dans clean_gdelt.py) = array ISO3 des pays
    # impliqués dans l'événement (dédupliqué, sans null).
    agg_exprs = [F.sum(c).alias(c) for c in SCORE_COLS] + [
        F.count("*").alias("event_count"),
        # max(struct(score, actor_countries)) → pays du pire événement de la tranche
        F.max(F.struct(
            F.col("geo_score_raw"),
            F.col("actor_countries"),
        )).alias("_best_actor_struct"),
    ]

    df_agg = df_gdelt.groupBy("DATEADDED").agg(*agg_exprs)

    # ── Extraire les pays depuis le struct ──
    df_agg = (
        df_agg
        .withColumn("actor_countries", F.col("_best_actor_struct.actor_countries"))
        .drop("_best_actor_struct")
    )

    count = df_agg.count()
    logger.info("GDELT agrégé : %d tranches de 15 min", count)
    return df_agg


# ──────────────────────────────────────────────
# ÉTAPE 2 — FULL OUTER JOIN + FLAG MARKET_OPEN
# ──────────────────────────────────────────────

def _full_join_wti_gdelt(df_wti: DataFrame, df_gdelt_agg: DataFrame) -> DataFrame:
    """
    Full outer join sur le timestamp 15 min.
      - master_datetime = coalesce(WTI.Datetime, GDELT.DATEADDED)
      - market_open = 1 si Close non null (bougie existe), 0 sinon
      - Scores GDELT nulls remplis par 0
    """
    logger.info("Étape 2 : Full outer join WTI × GDELT")

    # ── Cast WTI.Datetime en timestamp ──
    df_wti = df_wti.withColumn("wti_ts", F.col("Datetime").cast("timestamp"))

    # ── Join ──
    df = df_wti.join(
        df_gdelt_agg,
        df_wti["wti_ts"] == df_gdelt_agg["DATEADDED"],
        how="full_outer",
    )

    # ── master_datetime unifié ──
    df = df.withColumn(
        "master_datetime",
        F.coalesce(F.col("wti_ts"), F.col("DATEADDED")),
    )

    # ── Flag market_open : 1 si bougie WTI existante, 0 sinon ──
    df = df.withColumn(
        "market_open",
        F.when(F.col("Close").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
    )

    # ── Fill nulls des scores GDELT par 0 ──
    for c in SCORE_COLS + ["event_count"]:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))

    df = df.withColumn(
        "actor_countries",
        F.coalesce(F.col("actor_countries"), F.array().cast("array<string>")),
    )

    open_count = df.filter(F.col("market_open") == 1).count()
    total_count = df.count()
    logger.info("Join terminé : %d lignes (market_open=1 : %d, fermé : %d)",
                total_count, open_count, total_count - open_count)

    return df


# ──────────────────────────────────────────────
# ÉTAPE 3 — FORWARD MAPPING (fermeture → réouverture)
# ──────────────────────────────────────────────

def _forward_map_to_open(df: DataFrame) -> DataFrame:
    """
    Assigne à chaque ligne le datetime de la PROCHAINE bougie
    d'ouverture du marché via une Window triée en DESCENDANT.

    Logique :
      Window(orderBy DESC) + last(when(market_open==1, master_datetime))
      → Chaque ligne de week-end/nuit pointe vers la réouverture suivante.
      → Chaque ligne de marché ouvert pointe vers elle-même.
    """
    logger.info("Étape 3 : Forward mapping (fermeture → prochaine ouverture)")

    # Window triée en DESCENDANT → propage le prochain open vers les lignes passées
    w_desc = (
        Window.orderBy(F.col("master_datetime").desc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df = df.withColumn(
        "target_open_datetime",
        F.last(
            F.when(F.col("market_open") == 1, F.col("master_datetime")),
            ignorenulls=True,
        ).over(w_desc),
    )

    # Supprimer les lignes sans target (après la dernière ouverture connue)
    nulls = df.filter(F.col("target_open_datetime").isNull()).count()
    if nulls > 0:
        logger.warning(
            "%d lignes sans target_open_datetime (pas de prochaine ouverture connue) — supprimées",
            nulls,
        )
        df = df.filter(F.col("target_open_datetime").isNotNull())

    return df


# ──────────────────────────────────────────────
# ÉTAPE 4 — LISSAGE DES PÉRIODES DE FERMETURE
# ──────────────────────────────────────────────

def _smooth_closed_periods(df: DataFrame) -> DataFrame:
    """
    GroupBy target_open_datetime :
      - gap_duration_15m  = nombre de quarts d'heure accumulés
      - {score}_smoothed  = 0.25 × max(score) + 0.75 × mean(score)
        → Hybride : le pic de stress domine (25%), la moyenne contextualise (75%)
        → Évite la sous-estimation des weekends due au lissage linéaire
      - {score}_sum       = somme brute (pour le modèle ML)
      - total_event_count = somme du nombre d'événements
      - period_actor_countries = actor_countries du pire événement
    """
    logger.info("Étape 4 : Lissage hybride (0.25×max + 0.75×mean) sur les périodes de fermeture")

    ALPHA = 0.25  # poids du pic (max)

    # ── Agrégation par target_open_datetime ──
    agg_exprs = [
        F.count("*").alias("gap_duration_15m"),
        F.sum("event_count").alias("total_event_count"),
        F.max(F.struct(
            F.col("geo_score_raw"),
            F.col("actor_countries"),
        )).alias("_best_period_actor"),
    ]
    for c in SCORE_COLS:
        agg_exprs.append(F.sum(c).alias(f"{c}_sum"))
        agg_exprs.append(F.max(c).alias(f"{c}_max"))

    df_smoothed = df.groupBy("target_open_datetime").agg(*agg_exprs)

    # ── Score hybride = 0.8 × max + 0.2 × mean ──────────────────
    for c in SCORE_COLS:
        mean_col = F.col(f"{c}_sum") / F.col("gap_duration_15m")
        df_smoothed = df_smoothed.withColumn(
            f"{c}_smoothed",
            F.round(F.lit(ALPHA) * F.col(f"{c}_max") + F.lit(1 - ALPHA) * mean_col, 6),
        )

    # ── Extraire le period_actor_countries ──
    df_smoothed = (
        df_smoothed
        .withColumn("period_actor_countries", F.col("_best_period_actor.actor_countries"))
        .drop("_best_period_actor")
    )

    count = df_smoothed.count()
    logger.info("Lissage terminé : %d périodes", count)
    return df_smoothed


# ──────────────────────────────────────────────
# ÉTAPE 5 — JOINTURE FINALE + PERCENTILE 7 JOURS
# ──────────────────────────────────────────────

def _final_join_and_percentile(df_wti: DataFrame, df_smoothed: DataFrame) -> DataFrame:
    """
    Inner join WTI × scores lissés sur Datetime = target_open_datetime.
    → Ne garde QUE les bougies de marché ouvert (automatique via inner join).
    → Calcule le percentile glissant 7 jours de geo_score_raw_smoothed.
    """
    logger.info("Étape 5 : Jointure finale + percentile 7 jours")

    # ── Cast WTI.Datetime en timestamp ──
    df_wti = df_wti.withColumn("wti_ts", F.col("Datetime").cast("timestamp"))

    # ── Inner join : supprime automatiquement le hors-marché ──
    df_gold = df_wti.join(
        df_smoothed,
        df_wti["wti_ts"] == df_smoothed["target_open_datetime"],
        how="inner",
    )

    # ── Epoch seconds pour les fenêtres glissantes ──
    df_gold = df_gold.withColumn(
        "epoch_s", F.unix_timestamp(F.col("wti_ts")),
    )

    # ── Percentile glissant 7 jours ──────────────────────────────
    #
    # Pourquoi rowsBetween(-672, 0) ?
    #   672 ≈ 7 jours × 24h × 4 quarts par heure = max théorique de
    #   bougies de marché ouvert en 7 jours.
    #   rowsBetween est GLISSANT : chaque ligne a sa propre fenêtre.
    #
    # Pourquoi min-max et pas percent_rank() ?
    #   percent_rank() ne peut PAS être combiné avec rowsBetween/rangeBetween
    #   en PySpark. On utilise un min-max scaling dans la fenêtre glissante :
    #     score_pct_7d = (score - min_7d) / (max_7d - min_7d) × 100
    #   C'est l'équivalent continu d'un percentile (0 = min historique,
    #   100 = max historique sur 7j) et c'est plus adapté pour le ML.
    #
    w_7d = Window.orderBy("epoch_s").rowsBetween(-672, 0)

    min_score_7d = F.min("geo_score_raw_smoothed").over(w_7d)
    max_score_7d = F.max("geo_score_raw_smoothed").over(w_7d)

    df_gold = df_gold.withColumn(
        "score_pct_7d",
        F.round(
            F.when(
                max_score_7d > min_score_7d,
                ((F.col("geo_score_raw_smoothed") - min_score_7d)
                 / (max_score_7d - min_score_7d)) * 100,
            ).otherwise(F.lit(50.0)),   # tous les scores identiques → 50e percentile
            2,
        ),
    )

    # ── Sélection et nettoyage des colonnes finales ──
    df_gold = df_gold.select(
        # WTI
        F.col("Datetime"),
        F.col("Open"),
        F.col("High"),
        F.col("Low"),
        F.col("Close"),
        F.col("Volume"),
        F.col("Volatility_Range"),
        F.col("Variation_Pct"),

        # GDELT lissé
        F.col("geo_I_smoothed"),
        F.col("geo_B_smoothed"),
        F.col("geo_S_smoothed"),
        F.col("geo_score_raw_smoothed"),

        # GDELT brut (sommes accumulées + max pour calibration ALPHA)
        F.col("geo_I_sum"),
        F.col("geo_B_sum"),
        F.col("geo_S_sum"),
        F.col("geo_score_raw_sum"),
        F.col("geo_score_raw_max"),

        # Métadonnées
        F.col("total_event_count"),
        F.col("gap_duration_15m"),
        F.col("period_actor_countries"),

        # Percentile
        F.col("score_pct_7d"),
    )

    # ── Explode actor_countries : 1 ligne par pays ───────────────
    # explode_outer garde les lignes dont l'array est vide (→ null).
    # Les métriques utilisent des moyennes dans Kibana, donc
    # les lignes dupliquées n'affectent pas les aggrégations.
    df_gold = (
        df_gold
        .withColumn("period_actor_country",
                     F.explode_outer(F.col("period_actor_countries")))
        .drop("period_actor_countries")
    )

    # ── Tri chronologique ──
    df_gold = df_gold.orderBy("Datetime")

    count = df_gold.count()
    logger.info("Table Gold finale : %d lignes × %d colonnes", count, len(df_gold.columns))
    return df_gold


# ──────────────────────────────────────────────
# ÉCRITURE PARQUET
# ──────────────────────────────────────────────

def _write_parquet(df: DataFrame, path: str) -> None:
    """Écrit un DataFrame Spark en un seul fichier Parquet sur S3."""
    df = df.cache()
    count = df.count()
    df.coalesce(1).write.mode("overwrite").parquet(path)
    logger.info("Parquet écrit → %s (%d lignes)", path, count)
    df.unpersist()


# ──────────────────────────────────────────────
# 1. COMPUTE HISTORY (Construction complète)
# ──────────────────────────────────────────────

def compute_history() -> None:
    """
    Construction complète de la table Gold à partir de TOUT
    l'historique Silver (WTI + GDELT).
    À lancer UNE SEULE FOIS ou pour recalculer entièrement.
    """
    spark = _get_spark()

    logger.info("═" * 60)
    logger.info("COMPUTE STRESS INDEX — Mode history (complet)")
    logger.info("═" * 60)

    try:
        # ── Chargement Silver ──
        df_wti   = _load_wti(spark, "history", None)
        df_gdelt = _load_gdelt(spark, "history", None)

        # ── Pipeline Gold ──
        df_gdelt_agg = _aggregate_gdelt_15min(df_gdelt)
        df_joined    = _full_join_wti_gdelt(df_wti, df_gdelt_agg)
        df_mapped    = _forward_map_to_open(df_joined)
        df_smoothed  = _smooth_closed_periods(df_mapped)
        df_gold      = _final_join_and_percentile(df_wti, df_smoothed)

        # ── Écriture ──
        _write_parquet(df_gold, GOLD_OUTPUT_PATH)

        # ── Résumé ──
        dt_min = df_gold.agg(F.min("Datetime")).collect()[0][0]
        dt_max = df_gold.agg(F.max("Datetime")).collect()[0][0]
        logger.info("Plage temporelle Gold : %s → %s", dt_min, dt_max)
        logger.info("Compute history terminé ✅")

    except Exception as e:
        logger.error("Erreur lors du compute history : %s", e)
        raise
    finally:
        spark.stop()


# ──────────────────────────────────────────────
# 2. COMPUTE DAILY (Incrémental)
# ──────────────────────────────────────────────

def compute_daily(target_date: str) -> None:
    """
    Mise à jour incrémentale de la table Gold pour un jour donné.
    Charge 8 jours de contexte (7j percentile + 1j courant).

    Args:
        target_date: Date au format YYYY-MM-DD (injectée par Airflow via {{ ds }}).
    """
    spark = _get_spark()

    logger.info("═" * 60)
    logger.info("COMPUTE STRESS INDEX — Mode daily (incrémental)")
    logger.info("Date cible : %s", target_date)
    logger.info("═" * 60)

    try:
        # ── Chargement Silver (fenêtre de 8 jours) ──
        df_wti   = _load_wti(spark, "daily", target_date)
        df_gdelt = _load_gdelt(spark, "daily", target_date)

        if df_wti.count() == 0:
            logger.warning("Aucune donnée WTI pour la période — rien à faire.")
            spark.stop()
            return

        # ── Pipeline Gold (même logique que history, sur la fenêtre) ──
        df_gdelt_agg = _aggregate_gdelt_15min(df_gdelt)
        df_joined    = _full_join_wti_gdelt(df_wti, df_gdelt_agg)
        df_mapped    = _forward_map_to_open(df_joined)
        df_smoothed  = _smooth_closed_periods(df_mapped)
        df_gold      = _final_join_and_percentile(df_wti, df_smoothed)

        # ── Filtrer uniquement le jour cible pour l'écriture ──
        df_gold_day = df_gold.filter(
            F.to_date(F.col("Datetime")) == F.lit(target_date)
        )

        if df_gold_day.count() == 0:
            logger.info("Aucune bougie Gold pour %s (marché fermé ?) — rien à écrire.", target_date)
            spark.stop()
            return

        # ── Charger l'existant et fusionner ──
        try:
            df_existing = spark.read.parquet(GOLD_OUTPUT_PATH)
            # Supprimer les données du jour cible (idempotence)
            df_existing = df_existing.filter(
                F.to_date(F.col("Datetime")) != F.lit(target_date)
            )
            df_final = df_existing.unionByName(df_gold_day, allowMissingColumns=True)
        except Exception:
            logger.info("Aucun fichier Gold existant — création initiale.")
            df_final = df_gold_day

        df_final = df_final.orderBy("Datetime")
        _write_parquet(df_final, GOLD_OUTPUT_PATH)

        logger.info("Compute daily terminé ✅")

    except Exception as e:
        logger.error("Erreur lors du compute daily : %s", e)
        raise
    finally:
        spark.stop()


# ──────────────────────────────────────────────
# POINT D'ENTRÉE
# ──────────────────────────────────────────────

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Couche Gold — Stress Index Géopolitique × WTI (PySpark)"
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=["history", "daily"],
        help="Mode : 'history' pour la construction complète, 'daily' pour l'incrémental.",
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
        compute_daily(args.date)
    else:
        compute_history()
