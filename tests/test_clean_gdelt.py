"""
test_clean_gdelt.py
===================
Tests unitaires pour src/transformation/clean_gdelt.py.

Teste les fonctions pures/transformations PySpark en utilisant une SparkSession
locale (sans S3). Les tests vérifient la logique métier uniquement.

Couverture :
  - _country_class_expr : classification pays → classe (1..4)
  - _clip01             : bornage [0, 1]
  - _add_geo_scores     : calcul des 4 scores (geo_I, geo_B, geo_S, geo_score_raw)
  - _clean_dataframe    : pipeline complet (cast, dédoublonnage, filtre, scores)
"""

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

from src.transformation.clean_gdelt import (
    _add_geo_scores,
    _clean_dataframe,
    _clip01,
    _country_class_expr,
    FILTER_MIN_ARTICLES,
    FILTER_MIN_GOLDSTEIN,
    KEEP_COLS,
)


# ──────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """SparkSession locale pour les tests (sans S3)."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_clean_gdelt")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _make_gdelt_row(**overrides) -> dict:
    """Construit un dict représentant une ligne GDELT brute avec des valeurs par défaut."""
    base = {
        "GlobalEventID": "1234567890",
        "Day": "20260227",
        "DATEADDED": "20260227093000",
        "Actor1Code": "USA",
        "Actor1Name": "UNITED STATES",
        "Actor1CountryCode": "USA",
        "Actor1Type1Code": "GOV",
        "Actor2Code": "RUS",
        "Actor2Name": "RUSSIA",
        "Actor2CountryCode": "RUS",
        "Actor2Type1Code": "GOV",
        "EventCode": "190",
        "EventRootCode": "19",
        "QuadClass": "4",
        "GoldsteinScale": "-10.0",
        "IsRootEvent": "1",
        "ActionGeo_CountryCode": "IRQ",
        "ActionGeo_Lat": "33.0",
        "ActionGeo_Long": "44.0",
        "NumMentions": "50",
        "NumSources": "20",
        "NumArticles": "15",
        "AvgTone": "-5.5",
    }
    base.update(overrides)
    return base


# ──────────────────────────────────────────────
# _country_class_expr
# ──────────────────────────────────────────────


class TestCountryClassExpr:
    def test_game_changer_returns_4(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([Row(country="USA")])
        result = df.select(_country_class_expr("country").alias("cls")).first()
        assert result["cls"] == 4  # USA weight = 4 in COUNTRY_WEIGHTS_ISO3

    def test_pilier_offre_returns_6(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([Row(country="IRN")])
        result = df.select(_country_class_expr("country").alias("cls")).first()
        assert result["cls"] == 6

    def test_influence_indirecte_returns_3(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([Row(country="NGA")])
        result = df.select(_country_class_expr("country").alias("cls")).first()
        assert result["cls"] == 3

    def test_unknown_country_returns_1(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([Row(country="XYZ")])
        result = df.select(_country_class_expr("country").alias("cls")).first()
        assert result["cls"] == 1

    def test_null_returns_1(self, spark: SparkSession) -> None:
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField("country", StringType(), True)])
        df = spark.createDataFrame([(None,)], schema=schema)
        result = df.select(_country_class_expr("country").alias("cls")).first()
        assert result["cls"] == 1


# ──────────────────────────────────────────────
# _clip01
# ──────────────────────────────────────────────


class TestClip01:
    @pytest.mark.parametrize("val,expected", [
        (0.5, 0.5),
        (-0.3, 0.0),
        (1.5, 1.0),
        (0.0, 0.0),
        (1.0, 1.0),
    ])
    def test_clips_correctly(self, spark: SparkSession, val: float, expected: float) -> None:
        df = spark.createDataFrame([Row(x=val)])
        result = df.select(_clip01(F.col("x")).alias("clipped")).first()
        assert abs(result["clipped"] - expected) < 1e-6


# ──────────────────────────────────────────────
# _add_geo_scores
# ──────────────────────────────────────────────


class TestAddGeoScores:
    def test_adds_four_score_columns(self, spark: SparkSession) -> None:
        row = _make_gdelt_row()
        df = spark.createDataFrame([row])
        # Cast les colonnes requises
        df = (
            df
            .withColumn("NumArticles", F.col("NumArticles").cast("int"))
            .withColumn("GoldsteinScale", F.col("GoldsteinScale").cast("double"))
            .withColumn("AvgTone", F.col("AvgTone").cast("double"))
        )
        result = _add_geo_scores(df)
        cols = result.columns
        assert "geo_I" in cols
        assert "geo_B" in cols
        assert "geo_S" in cols
        assert "geo_score_raw" in cols
        assert "actor_countries" in cols

    def test_geo_scores_are_positive(self, spark: SparkSession) -> None:
        row = _make_gdelt_row()
        df = spark.createDataFrame([row])
        df = (
            df
            .withColumn("NumArticles", F.col("NumArticles").cast("int"))
            .withColumn("GoldsteinScale", F.col("GoldsteinScale").cast("double"))
            .withColumn("AvgTone", F.col("AvgTone").cast("double"))
        )
        result = _add_geo_scores(df).first()
        assert result["geo_I"] > 0
        assert result["geo_B"] > 0
        assert result["geo_S"] > 0
        assert result["geo_score_raw"] > 0

    def test_actor_countries_contains_all_unique(self, spark: SparkSession) -> None:
        # USA (A1), RUS (A2), IZ→IRQ (ActionGeo) → ["IRQ", "USA", "RUS"] (dédupliqué)
        row = _make_gdelt_row(
            Actor1CountryCode="USA",
            Actor2CountryCode="RUS",
            ActionGeo_CountryCode="IZ",
        )
        df = spark.createDataFrame([row])
        df = (
            df
            .withColumn("NumArticles", F.col("NumArticles").cast("int"))
            .withColumn("GoldsteinScale", F.col("GoldsteinScale").cast("double"))
            .withColumn("AvgTone", F.col("AvgTone").cast("double"))
        )
        result = _add_geo_scores(df).first()
        countries = sorted(result["actor_countries"])
        assert countries == ["IRQ", "RUS", "USA"]


# ──────────────────────────────────────────────
# _clean_dataframe
# ──────────────────────────────────────────────


class TestCleanDataframe:
    def test_filters_low_article_count(self, spark: SparkSession) -> None:
        """Événements avec NumArticles < 4 doivent être supprimés."""
        rows = [
            _make_gdelt_row(GlobalEventID="1", NumArticles="15"),
            _make_gdelt_row(GlobalEventID="2", NumArticles="2"),  # filtré
        ]
        df = spark.createDataFrame(rows)
        result = _clean_dataframe(df)
        assert result.count() == 1

    def test_filters_low_goldstein(self, spark: SparkSession) -> None:
        """Événements avec |GoldsteinScale| < 5 doivent être filtrés."""
        rows = [
            _make_gdelt_row(GlobalEventID="1", GoldsteinScale="-10.0"),
            _make_gdelt_row(GlobalEventID="2", GoldsteinScale="-2.0"),  # filtré
        ]
        df = spark.createDataFrame(rows)
        result = _clean_dataframe(df)
        assert result.count() == 1

    def test_deduplicates_on_global_event_id(self, spark: SparkSession) -> None:
        rows = [
            _make_gdelt_row(GlobalEventID="1"),
            _make_gdelt_row(GlobalEventID="1"),  # doublon → supprimé
        ]
        df = spark.createDataFrame(rows)
        result = _clean_dataframe(df)
        assert result.count() == 1

    def test_output_has_geo_scores(self, spark: SparkSession) -> None:
        rows = [_make_gdelt_row()]
        df = spark.createDataFrame(rows)
        result = _clean_dataframe(df)
        cols = result.columns
        for c in ["geo_I", "geo_B", "geo_S", "geo_score_raw", "actor_countries"]:
            assert c in cols, f"Colonne {c} manquante"

    def test_dateadded_parsed_as_timestamp(self, spark: SparkSession) -> None:
        rows = [_make_gdelt_row(DATEADDED="20260227093000")]
        df = spark.createDataFrame(rows)
        result = _clean_dataframe(df)
        row = result.first()
        assert row["DATEADDED"] is not None
        assert str(row["DATEADDED"]).startswith("2026-02-27")


# ──────────────────────────────────────────────
# _write_parquet (local)
# ──────────────────────────────────────────────


class TestWriteParquet:
    def test_writes_parquet_to_local_path(self, spark: SparkSession, tmp_path) -> None:
        from src.transformation.clean_gdelt import _write_parquet
        df = spark.createDataFrame([Row(x=1, y="a"), Row(x=2, y="b")])
        out = str(tmp_path / "test_out.parquet")
        _write_parquet(df, out)
        result = spark.read.parquet(out)
        assert result.count() == 2


# ──────────────────────────────────────────────
# format_history / format_daily — mocked I/O
# ──────────────────────────────────────────────

from unittest.mock import patch, MagicMock


class TestFormatHistory:
    @patch("boto3.client")
    @patch("src.transformation.clean_gdelt._get_spark")
    def test_returns_early_when_no_folders(self, mock_spark, mock_boto3_client) -> None:
        from src.transformation.clean_gdelt import format_history
        # Set up paginator to return no CommonPrefixes
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"CommonPrefixes": []}]

        format_history()
        # _get_spark should not be called if no folders
        mock_spark.assert_not_called()

    @patch("boto3.client")
    @patch("src.transformation.clean_gdelt._get_spark")
    def test_processes_batches(self, mock_spark, mock_boto3_client) -> None:
        from src.transformation.clean_gdelt import format_history
        # 2 date folders
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{
            "CommonPrefixes": [
                {"Prefix": "raw/gdelt/history/2026-01-04/"},
                {"Prefix": "raw/gdelt/history/2026-01-05/"},
            ]
        }]

        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_df = MagicMock()
        mock_session.read.option.return_value = mock_session.read
        mock_session.read.parquet.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.dropDuplicates.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.orderBy.return_value = mock_df
        mock_df.write.mode.return_value = mock_df
        mock_df.parquet = MagicMock()
        mock_session.catalog.clearCache = MagicMock()
        mock_session.read.parquet.return_value = mock_df
        mock_df.count.return_value = 100

        format_history()
        assert mock_spark.call_count >= 1


class TestFormatDaily:
    @patch("src.transformation.clean_gdelt._write_parquet")
    @patch("src.transformation.clean_gdelt._clean_dataframe")
    @patch("src.transformation.clean_gdelt._get_spark")
    def test_stops_when_no_existing_parquet(self, mock_spark, mock_clean, mock_write) -> None:
        from src.transformation.clean_gdelt import format_daily
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_session.read.parquet.side_effect = Exception("not found")

        with pytest.raises(FileNotFoundError):
            format_daily("2026-02-27")
        mock_session.stop.assert_called_once()
        mock_clean.assert_not_called()

    @patch("src.transformation.clean_gdelt._write_parquet")
    @patch("src.transformation.clean_gdelt._clean_dataframe")
    @patch("src.transformation.clean_gdelt._get_spark")
    def test_stops_when_no_daily_files(self, mock_spark, mock_clean, mock_write) -> None:
        from src.transformation.clean_gdelt import format_daily
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_existing = MagicMock()
        mock_session.read.parquet.side_effect = [mock_existing, Exception("no daily")]

        format_daily("2026-02-27")
        mock_session.stop.assert_called_once()
        mock_clean.assert_not_called()

    @patch("src.transformation.clean_gdelt._write_parquet")
    @patch("src.transformation.clean_gdelt._clean_dataframe")
    @patch("src.transformation.clean_gdelt._get_spark")
    def test_merges_and_writes(self, mock_spark, mock_clean, mock_write) -> None:
        from src.transformation.clean_gdelt import format_daily
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_existing = MagicMock()
        mock_new = MagicMock()
        mock_session.read.parquet.side_effect = [mock_existing, mock_new]

        # _clean_dataframe is called on df_new only
        mock_cleaned_new = MagicMock()
        mock_clean.return_value = mock_cleaned_new

        # unionByName is called on df_existing with cleaned new data
        mock_merged = MagicMock()
        mock_existing.unionByName.return_value = mock_merged
        mock_deduped = MagicMock()
        mock_merged.dropDuplicates.return_value = mock_deduped
        mock_ordered = MagicMock()
        mock_deduped.orderBy.return_value = mock_ordered

        # agg().collect()[0] returns a Row-like tuple
        mock_agg_result = MagicMock()
        mock_agg_result.__getitem__ = MagicMock(side_effect=lambda i: ["2026-01-01", "2026-02-27"][i])
        mock_agg_result.__iter__ = MagicMock(return_value=iter(["2026-01-01", "2026-02-27"]))
        mock_ordered.agg.return_value.collect.return_value = [mock_agg_result]

        format_daily("2026-02-27")
        mock_write.assert_called_once()
        mock_session.stop.assert_called_once()
