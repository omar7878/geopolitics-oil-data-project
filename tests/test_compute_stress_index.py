"""
test_compute_stress_index.py
============================
Tests unitaires pour src/combination/compute_stress_index.py.

Teste les fonctions pures PySpark du pipeline Gold (Stress Index) en utilisant
une SparkSession locale (sans S3). Vérifie la logique métier de chaque étape.

Couverture :
  - _aggregate_gdelt_15min     : somme scores, actor_countries, event_count
  - _full_join_wti_gdelt       : join + market_open flag + fill nulls
  - _forward_map_to_open       : mapping fermeture → prochaine ouverture
  - _smooth_closed_periods     : lissage hybride 0.8×max + 0.2×mean
  - _final_join_and_percentile : inner join + score_pct_7d
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    LongType,
    IntegerType,
    StringType,
    TimestampType,
)

from src.combination.compute_stress_index import (
    _aggregate_gdelt_15min,
    _full_join_wti_gdelt,
    _forward_map_to_open,
    _smooth_closed_periods,
    _final_join_and_percentile,
    SCORE_COLS,
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
        .appName("test_compute_stress_index")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _ts(s: str) -> datetime:
    """Raccourci pour créer un datetime depuis une string."""
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


# ──────────────────────────────────────────────
# _aggregate_gdelt_15min
# ──────────────────────────────────────────────


class TestAggregateGdelt15min:
    def test_sums_scores_per_timestamp(self, spark: SparkSession) -> None:
        data = [
            Row(DATEADDED=_ts("2026-02-27 09:00:00"), geo_I=1.0, geo_B=2.0,
                geo_S=1.5, geo_score_raw=10.0, actor_countries=["USA"]),
            Row(DATEADDED=_ts("2026-02-27 09:00:00"), geo_I=2.0, geo_B=1.0,
                geo_S=2.0, geo_score_raw=8.0, actor_countries=["RUS"]),
            Row(DATEADDED=_ts("2026-02-27 09:15:00"), geo_I=3.0, geo_B=3.0,
                geo_S=3.0, geo_score_raw=20.0, actor_countries=["CHN"]),
        ]
        df = spark.createDataFrame(data)
        result = _aggregate_gdelt_15min(df)

        assert result.count() == 2

        row_09 = result.filter(F.col("DATEADDED") == _ts("2026-02-27 09:00:00")).first()
        assert row_09["geo_I"] == 3.0   # 1 + 2
        assert row_09["geo_B"] == 3.0   # 2 + 1
        assert row_09["event_count"] == 2

    def test_actor_countries_from_best_score(self, spark: SparkSession) -> None:
        data = [
            Row(DATEADDED=_ts("2026-02-27 09:00:00"), geo_I=1.0, geo_B=2.0,
                geo_S=1.5, geo_score_raw=10.0, actor_countries=["USA"]),
            Row(DATEADDED=_ts("2026-02-27 09:00:00"), geo_I=2.0, geo_B=1.0,
                geo_S=2.0, geo_score_raw=50.0, actor_countries=["RUS", "IRN"]),
        ]
        df = spark.createDataFrame(data)
        result = _aggregate_gdelt_15min(df)
        row = result.first()
        # RUS/IRN a le geo_score_raw le plus élevé (50 > 10) → actor_countries = ["RUS", "IRN"]
        assert sorted(row["actor_countries"]) == ["IRN", "RUS"]


# ──────────────────────────────────────────────
# _full_join_wti_gdelt
# ──────────────────────────────────────────────


class TestFullJoinWtiGdelt:
    def test_market_open_flag(self, spark: SparkSession) -> None:
        wti_data = [
            Row(Datetime=_ts("2026-02-27 09:00:00"), Open=62.9, High=63.0,
                Low=62.8, Close=62.93, Volume=120,
                Volatility_Range=0.2, Variation_Pct=0.0),
        ]
        gdelt_data = [
            Row(DATEADDED=_ts("2026-02-27 09:00:00"), geo_I=1.0, geo_B=2.0,
                geo_S=1.5, geo_score_raw=10.0, event_count=5, actor_countries=["USA"]),
            Row(DATEADDED=_ts("2026-02-27 03:00:00"), geo_I=0.5, geo_B=1.0,
                geo_S=0.8, geo_score_raw=4.0, event_count=2, actor_countries=["RUS"]),
        ]
        df_wti = spark.createDataFrame(wti_data)
        df_gdelt = spark.createDataFrame(gdelt_data)
        result = _full_join_wti_gdelt(df_wti, df_gdelt)

        # 09:00 a Close → market_open=1
        row_open = result.filter(F.col("master_datetime") == _ts("2026-02-27 09:00:00")).first()
        assert row_open["market_open"] == 1

        # 03:00 n'a pas de Close → market_open=0
        row_closed = result.filter(F.col("master_datetime") == _ts("2026-02-27 03:00:00")).first()
        assert row_closed["market_open"] == 0

    def test_fills_null_scores_with_zero(self, spark: SparkSession) -> None:
        wti_data = [
            Row(Datetime=_ts("2026-02-27 09:00:00"), Open=62.9, High=63.0,
                Low=62.8, Close=62.93, Volume=120,
                Volatility_Range=0.2, Variation_Pct=0.0),
        ]
        gdelt_data = []  # pas de GDELT pour ce timestamp
        df_wti = spark.createDataFrame(wti_data)
        from pyspark.sql.types import ArrayType
        schema_gdelt = StructType([
            StructField("DATEADDED", TimestampType()),
            StructField("geo_I", DoubleType()),
            StructField("geo_B", DoubleType()),
            StructField("geo_S", DoubleType()),
            StructField("geo_score_raw", DoubleType()),
            StructField("event_count", DoubleType()),
            StructField("actor_countries", ArrayType(StringType())),
        ])
        df_gdelt = spark.createDataFrame(gdelt_data, schema=schema_gdelt)
        result = _full_join_wti_gdelt(df_wti, df_gdelt)
        row = result.first()
        for c in SCORE_COLS:
            assert row[c] == 0.0


# ──────────────────────────────────────────────
# _forward_map_to_open
# ──────────────────────────────────────────────


class TestForwardMapToOpen:
    def test_maps_closed_to_next_open(self, spark: SparkSession) -> None:
        data = [
            Row(master_datetime=_ts("2026-02-27 03:00:00"), market_open=0, Close=None, geo_score_raw=5.0),
            Row(master_datetime=_ts("2026-02-27 03:15:00"), market_open=0, Close=None, geo_score_raw=3.0),
            Row(master_datetime=_ts("2026-02-27 09:00:00"), market_open=1, Close=62.93, geo_score_raw=10.0),
        ]
        df = spark.createDataFrame(data)
        result = _forward_map_to_open(df)

        # Toutes les lignes doivent pointer vers 09:00
        targets = [row["target_open_datetime"] for row in result.collect()]
        assert all(t == _ts("2026-02-27 09:00:00") for t in targets)

    def test_open_maps_to_itself(self, spark: SparkSession) -> None:
        data = [
            Row(master_datetime=_ts("2026-02-27 09:00:00"), market_open=1, Close=62.93, geo_score_raw=10.0),
            Row(master_datetime=_ts("2026-02-27 09:15:00"), market_open=1, Close=62.95, geo_score_raw=8.0),
        ]
        df = spark.createDataFrame(data)
        result = _forward_map_to_open(df)

        rows = result.orderBy("master_datetime").collect()
        assert rows[0]["target_open_datetime"] == _ts("2026-02-27 09:00:00")
        assert rows[1]["target_open_datetime"] == _ts("2026-02-27 09:15:00")


# ──────────────────────────────────────────────
# _smooth_closed_periods
# ──────────────────────────────────────────────


class TestSmoothClosedPeriods:
    def test_hybride_formula(self, spark: SparkSession) -> None:
        """Vérifie le lissage 0.25×max + 0.75×mean."""
        data = [
            Row(target_open_datetime=_ts("2026-02-27 09:00:00"),
                geo_I=1.0, geo_B=2.0, geo_S=1.0, geo_score_raw=10.0,
                event_count=3, actor_countries=["USA"]),
            Row(target_open_datetime=_ts("2026-02-27 09:00:00"),
                geo_I=2.0, geo_B=4.0, geo_S=2.0, geo_score_raw=20.0,
                event_count=5, actor_countries=["USA"]),
        ]
        df = spark.createDataFrame(data)
        result = _smooth_closed_periods(df)
        row = result.first()

        # geo_score_raw : max=20, sum=30, gap=2, mean=15
        # smoothed = 0.25×20 + 0.75×15 = 5+11.25 = 16.25
        assert abs(row["geo_score_raw_smoothed"] - 16.25) < 0.01

    def test_gap_duration_15m(self, spark: SparkSession) -> None:
        data = [
            Row(target_open_datetime=_ts("2026-02-27 09:00:00"),
                geo_I=1.0, geo_B=1.0, geo_S=1.0, geo_score_raw=5.0,
                event_count=1, actor_countries=["USA"]),
            Row(target_open_datetime=_ts("2026-02-27 09:00:00"),
                geo_I=1.0, geo_B=1.0, geo_S=1.0, geo_score_raw=5.0,
                event_count=1, actor_countries=["USA"]),
            Row(target_open_datetime=_ts("2026-02-27 09:00:00"),
                geo_I=1.0, geo_B=1.0, geo_S=1.0, geo_score_raw=5.0,
                event_count=1, actor_countries=["USA"]),
        ]
        df = spark.createDataFrame(data)
        result = _smooth_closed_periods(df)
        row = result.first()
        assert row["gap_duration_15m"] == 3


# ──────────────────────────────────────────────
# _final_join_and_percentile
# ──────────────────────────────────────────────


class TestFinalJoinAndPercentile:
    def test_output_has_score_pct_7d(self, spark: SparkSession) -> None:
        wti_data = [
            Row(Datetime=_ts("2026-02-27 09:00:00"), Open=62.9, High=63.0,
                Low=62.8, Close=62.93, Volume=120,
                Volatility_Range=0.2, Variation_Pct=0.0),
            Row(Datetime=_ts("2026-02-27 09:15:00"), Open=62.93, High=63.05,
                Low=62.90, Close=62.98, Volume=95,
                Volatility_Range=0.15, Variation_Pct=0.08),
        ]
        smoothed_data = [
            Row(target_open_datetime=_ts("2026-02-27 09:00:00"),
                geo_I_smoothed=2.0, geo_B_smoothed=3.0,
                geo_S_smoothed=1.5, geo_score_raw_smoothed=15.0,
                geo_I_sum=4.0, geo_B_sum=6.0, geo_S_sum=3.0, geo_score_raw_sum=30.0,
                geo_I_max=3.0, geo_B_max=4.0, geo_S_max=2.0, geo_score_raw_max=20.0,
                total_event_count=10.0, gap_duration_15m=5,
                period_actor_countries=["USA"]),
            Row(target_open_datetime=_ts("2026-02-27 09:15:00"),
                geo_I_smoothed=1.5, geo_B_smoothed=2.5,
                geo_S_smoothed=1.2, geo_score_raw_smoothed=10.0,
                geo_I_sum=3.0, geo_B_sum=5.0, geo_S_sum=2.4, geo_score_raw_sum=20.0,
                geo_I_max=2.0, geo_B_max=3.5, geo_S_max=1.8, geo_score_raw_max=15.0,
                total_event_count=8.0, gap_duration_15m=1,
                period_actor_countries=["RUS"]),
        ]
        df_wti = spark.createDataFrame(wti_data)
        df_smoothed = spark.createDataFrame(smoothed_data)
        result = _final_join_and_percentile(df_wti, df_smoothed)

        assert "score_pct_7d" in result.columns
        assert result.count() == 2

    def test_only_market_open_rows(self, spark: SparkSession) -> None:
        """Inner join → pas de lignes hors-marché."""
        wti_data = [
            Row(Datetime=_ts("2026-02-27 09:00:00"), Open=62.9, High=63.0,
                Low=62.8, Close=62.93, Volume=120,
                Volatility_Range=0.2, Variation_Pct=0.0),
        ]
        smoothed_data = [
            Row(target_open_datetime=_ts("2026-02-27 09:00:00"),
                geo_I_smoothed=2.0, geo_B_smoothed=3.0,
                geo_S_smoothed=1.5, geo_score_raw_smoothed=15.0,
                geo_I_sum=4.0, geo_B_sum=6.0, geo_S_sum=3.0, geo_score_raw_sum=30.0,
                geo_I_max=3.0, geo_B_max=4.0, geo_S_max=2.0, geo_score_raw_max=20.0,
                total_event_count=10.0, gap_duration_15m=5,
                period_actor_countries=["USA"]),
            Row(target_open_datetime=_ts("2026-02-27 03:00:00"),  # hors-marché → exclu
                geo_I_smoothed=1.0, geo_B_smoothed=1.0,
                geo_S_smoothed=1.0, geo_score_raw_smoothed=5.0,
                geo_I_sum=2.0, geo_B_sum=2.0, geo_S_sum=2.0, geo_score_raw_sum=10.0,
                geo_I_max=1.5, geo_B_max=1.5, geo_S_max=1.5, geo_score_raw_max=8.0,
                total_event_count=4.0, gap_duration_15m=1,
                period_actor_countries=["RUS"]),
        ]
        df_wti = spark.createDataFrame(wti_data)
        df_smoothed = spark.createDataFrame(smoothed_data)
        result = _final_join_and_percentile(df_wti, df_smoothed)
        # 03:00 n'a pas de bougie WTI → exclue par inner join
        assert result.count() == 1


# ──────────────────────────────────────────────
# _load_wti / _load_gdelt — mocked I/O
# ──────────────────────────────────────────────

from unittest.mock import patch, MagicMock


class TestLoadWti:
    @patch("src.combination.compute_stress_index._get_spark")
    def test_loads_full_in_history_mode(self, mock_spark) -> None:
        from src.combination.compute_stress_index import _load_wti
        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_session.read.parquet.return_value = mock_df
        result = _load_wti(mock_session, "history", None)
        assert result.count() == 500
        mock_df.filter.assert_not_called()

    @patch("src.combination.compute_stress_index._get_spark")
    def test_filters_in_daily_mode(self, mock_spark) -> None:
        from src.combination.compute_stress_index import _load_wti
        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_filtered = MagicMock()
        mock_filtered.count.return_value = 50
        mock_df.filter.return_value = mock_filtered
        mock_session.read.parquet.return_value = mock_df

        result = _load_wti(mock_session, "daily", "2026-02-27")
        mock_df.filter.assert_called_once()
        assert result.count() == 50


class TestLoadGdelt:
    @patch("src.combination.compute_stress_index._get_spark")
    def test_loads_full_in_history_mode(self, mock_spark) -> None:
        from src.combination.compute_stress_index import _load_gdelt
        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_session.read.parquet.return_value = mock_df
        result = _load_gdelt(mock_session, "history", None)
        assert result.count() == 1000
        mock_df.filter.assert_not_called()

    @patch("src.combination.compute_stress_index._get_spark")
    def test_filters_in_daily_mode(self, mock_spark) -> None:
        from src.combination.compute_stress_index import _load_gdelt
        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_filtered = MagicMock()
        mock_filtered.count.return_value = 80
        mock_df.filter.return_value = mock_filtered
        mock_session.read.parquet.return_value = mock_df

        result = _load_gdelt(mock_session, "daily", "2026-02-27")
        mock_df.filter.assert_called_once()
        assert result.count() == 80


# ──────────────────────────────────────────────
# _write_parquet (mocked)
# ──────────────────────────────────────────────


class TestWriteParquet:
    def test_writes_parquet_to_path(self, spark: SparkSession, tmp_path) -> None:
        from src.combination.compute_stress_index import _write_parquet
        df = spark.createDataFrame([Row(x=1), Row(x=2)])
        out = str(tmp_path / "test_stress_out.parquet")
        _write_parquet(df, out)
        result = spark.read.parquet(out)
        assert result.count() == 2


# ──────────────────────────────────────────────
# compute_history / compute_daily — mocked I/O
# ──────────────────────────────────────────────


class TestComputeHistory:
    @patch("src.combination.compute_stress_index._write_parquet")
    @patch("src.combination.compute_stress_index._final_join_and_percentile")
    @patch("src.combination.compute_stress_index._smooth_closed_periods")
    @patch("src.combination.compute_stress_index._forward_map_to_open")
    @patch("src.combination.compute_stress_index._full_join_wti_gdelt")
    @patch("src.combination.compute_stress_index._aggregate_gdelt_15min")
    @patch("src.combination.compute_stress_index._load_gdelt")
    @patch("src.combination.compute_stress_index._load_wti")
    @patch("src.combination.compute_stress_index._get_spark")
    def test_calls_full_pipeline(self, mock_spark, mock_wti, mock_gdelt,
                                  mock_agg, mock_join, mock_fwd, mock_smooth,
                                  mock_final, mock_write) -> None:
        from src.combination.compute_stress_index import compute_history
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_gold = MagicMock()
        mock_final.return_value = mock_gold
        mock_gold.agg.return_value.collect.return_value = [("2026-01-04", "2026-02-27")]

        compute_history()
        mock_wti.assert_called_once()
        mock_gdelt.assert_called_once()
        mock_agg.assert_called_once()
        mock_join.assert_called_once()
        mock_fwd.assert_called_once()
        mock_smooth.assert_called_once()
        mock_final.assert_called_once()
        mock_write.assert_called_once()
        mock_session.stop.assert_called_once()


class TestComputeDaily:
    @patch("src.combination.compute_stress_index._write_parquet")
    @patch("src.combination.compute_stress_index._final_join_and_percentile")
    @patch("src.combination.compute_stress_index._smooth_closed_periods")
    @patch("src.combination.compute_stress_index._forward_map_to_open")
    @patch("src.combination.compute_stress_index._full_join_wti_gdelt")
    @patch("src.combination.compute_stress_index._aggregate_gdelt_15min")
    @patch("src.combination.compute_stress_index._load_gdelt")
    @patch("src.combination.compute_stress_index._load_wti")
    @patch("src.combination.compute_stress_index._get_spark")
    def test_returns_early_when_no_wti(self, mock_spark, mock_wti, mock_gdelt,
                                       mock_agg, mock_join, mock_fwd, mock_smooth,
                                       mock_final, mock_write) -> None:
        from src.combination.compute_stress_index import compute_daily
        mock_session = MagicMock()
        mock_spark.return_value = mock_session
        mock_df_wti = MagicMock()
        mock_df_wti.count.return_value = 0
        mock_wti.return_value = mock_df_wti

        compute_daily("2026-02-27")
        mock_session.stop.assert_called()
        mock_agg.assert_not_called()

    @patch("src.combination.compute_stress_index._write_parquet")
    @patch("src.combination.compute_stress_index._final_join_and_percentile")
    @patch("src.combination.compute_stress_index._smooth_closed_periods")
    @patch("src.combination.compute_stress_index._forward_map_to_open")
    @patch("src.combination.compute_stress_index._full_join_wti_gdelt")
    @patch("src.combination.compute_stress_index._aggregate_gdelt_15min")
    @patch("src.combination.compute_stress_index._load_gdelt")
    @patch("src.combination.compute_stress_index._load_wti")
    @patch("src.combination.compute_stress_index._get_spark")
    def test_full_daily_pipeline(self, mock_spark, mock_wti, mock_gdelt,
                                  mock_agg, mock_join, mock_fwd, mock_smooth,
                                  mock_final, mock_write) -> None:
        from src.combination.compute_stress_index import compute_daily
        mock_session = MagicMock()
        mock_spark.return_value = mock_session

        mock_df_wti = MagicMock()
        mock_df_wti.count.return_value = 100
        mock_wti.return_value = mock_df_wti

        mock_gold = MagicMock()
        mock_final.return_value = mock_gold
        mock_gold_day = MagicMock()
        mock_gold.filter.return_value = mock_gold_day
        mock_gold_day.count.return_value = 20

        # No existing gold file
        mock_session.read.parquet.side_effect = Exception("no existing")

        mock_gold_day.orderBy.return_value = mock_gold_day

        compute_daily("2026-02-27")
        mock_write.assert_called_once()
        mock_session.stop.assert_called_once()
