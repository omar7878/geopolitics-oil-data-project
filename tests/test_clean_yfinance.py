"""
test_clean_yfinance.py
======================
Tests unitaires pour src/transformation/clean_yfinance.py.

Teste les fonctions pures/transformations PySpark en utilisant une SparkSession
locale (sans S3). Les tests vérifient la logique métier uniquement.

Couverture :
  - _clean_dataframe : cast types, dédoublonnage Datetime, enrichissement Silver
                       (Volatility_Range, Variation_Pct), tri chronologique
  - _write_parquet   : écriture locale (test basique)
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
)

from src.transformation.clean_yfinance import _clean_dataframe


# ──────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """SparkSession locale pour les tests (sans S3)."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_clean_yfinance")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _make_wti_rows(spark: SparkSession, rows: list[dict]) -> "DataFrame":
    """Crée un DataFrame Spark à partir de dicts WTI."""
    schema = StructType([
        StructField("Datetime", StringType(), True),
        StructField("Open", StringType(), True),
        StructField("High", StringType(), True),
        StructField("Low", StringType(), True),
        StructField("Close", StringType(), True),
        StructField("Volume", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema=schema)


# ──────────────────────────────────────────────
# _clean_dataframe
# ──────────────────────────────────────────────


class TestCleanDataframe:
    def test_casts_numeric_types(self, spark: SparkSession) -> None:
        df = _make_wti_rows(spark, [
            {"Datetime": "2026-02-27 09:30:00", "Open": "62.90", "High": "62.95",
             "Low": "62.85", "Close": "62.93", "Volume": "120"},
        ])
        result = _clean_dataframe(df)
        schema = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema["Open"], DoubleType)
        assert isinstance(schema["Close"], DoubleType)
        assert isinstance(schema["Volume"], LongType)

    def test_deduplicates_on_datetime(self, spark: SparkSession) -> None:
        df = _make_wti_rows(spark, [
            {"Datetime": "2026-02-27 09:30:00", "Open": "62.90", "High": "62.95",
             "Low": "62.85", "Close": "62.93", "Volume": "120"},
            {"Datetime": "2026-02-27 09:30:00", "Open": "62.91", "High": "62.96",
             "Low": "62.86", "Close": "62.94", "Volume": "130"},  # doublon
        ])
        result = _clean_dataframe(df)
        assert result.count() == 1

    def test_adds_volatility_range(self, spark: SparkSession) -> None:
        df = _make_wti_rows(spark, [
            {"Datetime": "2026-02-27 09:30:00", "Open": "62.90", "High": "63.00",
             "Low": "62.80", "Close": "62.93", "Volume": "120"},
        ])
        result = _clean_dataframe(df)
        row = result.first()
        assert "Volatility_Range" in result.columns
        assert abs(row["Volatility_Range"] - 0.20) < 0.01

    def test_adds_variation_pct(self, spark: SparkSession) -> None:
        df = _make_wti_rows(spark, [
            {"Datetime": "2026-02-27 09:30:00", "Open": "62.90", "High": "62.95",
             "Low": "62.85", "Close": "100.00", "Volume": "120"},
            {"Datetime": "2026-02-27 09:45:00", "Open": "62.95", "High": "63.00",
             "Low": "62.90", "Close": "101.00", "Volume": "95"},
        ])
        result = _clean_dataframe(df)
        rows = result.orderBy("Datetime").collect()

        # Première ligne : pas de t-1 → null
        assert rows[0]["Variation_Pct"] is None

        # Deuxième ligne : (101 - 100) / 100 * 100 = 1.0 %
        assert abs(rows[1]["Variation_Pct"] - 1.0) < 0.01

    def test_output_sorted_by_datetime(self, spark: SparkSession) -> None:
        df = _make_wti_rows(spark, [
            {"Datetime": "2026-02-27 10:00:00", "Open": "63", "High": "63.05",
             "Low": "62.95", "Close": "63.02", "Volume": "110"},
            {"Datetime": "2026-02-27 09:30:00", "Open": "62.90", "High": "62.95",
             "Low": "62.85", "Close": "62.93", "Volume": "120"},
        ])
        result = _clean_dataframe(df)
        rows = result.collect()
        assert str(rows[0]["Datetime"]) < str(rows[1]["Datetime"])

    def test_preserves_all_expected_columns(self, spark: SparkSession) -> None:
        df = _make_wti_rows(spark, [
            {"Datetime": "2026-02-27 09:30:00", "Open": "62.90", "High": "62.95",
             "Low": "62.85", "Close": "62.93", "Volume": "120"},
        ])
        result = _clean_dataframe(df)
        expected = {"Datetime", "Open", "High", "Low", "Close", "Volume",
                    "Volatility_Range", "Variation_Pct"}
        assert expected.issubset(set(result.columns))


# ──────────────────────────────────────────────
# _write_parquet (local)
# ──────────────────────────────────────────────


class TestWriteParquet:
    def test_writes_parquet_to_local_path(self, spark: SparkSession, tmp_path) -> None:
        from src.transformation.clean_yfinance import _write_parquet
        df = spark.createDataFrame([Row(x=1, y="a"), Row(x=2, y="b")])
        out = str(tmp_path / "test_wti_out.parquet")
        _write_parquet(df, out)
        result = spark.read.parquet(out)
        assert result.count() == 2


# ──────────────────────────────────────────────
# format_history / format_daily — mocked I/O
# ──────────────────────────────────────────────

from unittest.mock import patch, MagicMock


class TestFormatHistory:
    @patch("src.transformation.clean_yfinance._write_parquet")
    @patch("src.transformation.clean_yfinance._clean_dataframe")
    @patch("src.transformation.clean_yfinance._get_spark")
    def test_returns_early_when_no_raw_files(self, mock_spark, mock_clean, mock_write) -> None:
        from src.transformation.clean_yfinance import format_history
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_session.read.option.return_value = mock_session.read
        mock_session.read.parquet.side_effect = Exception("no files")

        format_history()
        mock_session.stop.assert_called_once()
        mock_clean.assert_not_called()

    @patch("src.transformation.clean_yfinance._write_parquet")
    @patch("src.transformation.clean_yfinance._clean_dataframe")
    @patch("src.transformation.clean_yfinance._get_spark")
    def test_reads_cleans_and_writes(self, mock_spark, mock_clean, mock_write) -> None:
        from src.transformation.clean_yfinance import format_history
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_df = MagicMock()
        mock_session.read.option.return_value = mock_session.read
        mock_session.read.parquet.return_value = mock_df
        mock_df.count.return_value = 50
        mock_cleaned = MagicMock()
        mock_clean.return_value = mock_cleaned
        mock_cleaned.count.return_value = 48
        mock_cleaned.agg.return_value.collect.return_value = [("2026-01-04",)]

        format_history()
        mock_clean.assert_called_once_with(mock_df)
        mock_write.assert_called_once()
        mock_session.stop.assert_called_once()


class TestFormatDaily:
    @patch("src.transformation.clean_yfinance._write_parquet")
    @patch("src.transformation.clean_yfinance._clean_dataframe")
    @patch("src.transformation.clean_yfinance._get_spark")
    def test_stops_when_no_existing_parquet(self, mock_spark, mock_clean, mock_write) -> None:
        from src.transformation.clean_yfinance import format_daily
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_session.read.parquet.side_effect = Exception("not found")

        with pytest.raises(FileNotFoundError):
            format_daily("2026-02-27")
        mock_session.stop.assert_called_once()
        mock_clean.assert_not_called()

    @patch("src.transformation.clean_yfinance._write_parquet")
    @patch("src.transformation.clean_yfinance._clean_dataframe")
    @patch("src.transformation.clean_yfinance._get_spark")
    def test_stops_when_no_daily_files(self, mock_spark, mock_clean, mock_write) -> None:
        from src.transformation.clean_yfinance import format_daily
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_existing = MagicMock()
        mock_existing.count.return_value = 100
        mock_session.read.parquet.side_effect = [mock_existing, Exception("no daily")]

        format_daily("2026-02-27")
        mock_session.stop.assert_called_once()
        mock_clean.assert_not_called()

    @patch("src.transformation.clean_yfinance._write_parquet")
    @patch("src.transformation.clean_yfinance._clean_dataframe")
    @patch("src.transformation.clean_yfinance._get_spark")
    def test_merges_and_writes(self, mock_spark, mock_clean, mock_write) -> None:
        from src.transformation.clean_yfinance import format_daily
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_existing = MagicMock()
        mock_existing.count.return_value = 100
        mock_new = MagicMock()
        mock_new.count.return_value = 10
        mock_session.read.parquet.side_effect = [mock_existing, mock_new]
        mock_merged = MagicMock()
        mock_merged.count.return_value = 110
        mock_existing.unionByName.return_value = mock_merged
        mock_cleaned = MagicMock()
        mock_cleaned.count.return_value = 108
        mock_clean.return_value = mock_cleaned
        mock_cleaned.agg.return_value.collect.return_value = [("2026-01-04", "2026-02-27")]

        format_daily("2026-02-27")
        mock_write.assert_called_once()
        mock_session.stop.assert_called_once()
