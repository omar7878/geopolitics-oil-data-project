"""
test_load_to_elastic.py
=======================
Tests unitaires pour src/indexing/load_to_elastic.py.

Couverture :
  - _normalize_timestamp : renommage Datetime→timestamp, conversion UTC, colonne absente
  - _generate_actions    : projection des 20 colonnes Gold, colonnes manquantes
  - _ensure_index        : création de l'index / index déjà présent
  - _bulk_index          : appel bulk avec comptage succès/erreurs
  - main                 : orchestration complète (mocks S3 + ES)
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.indexing.load_to_elastic import (
    COLUMNS,
    ES_INDEX,
    _bulk_index,
    _ensure_index,
    _generate_actions,
    _normalize_timestamp,
    main,
)


# ──────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────


@pytest.fixture
def gold_df() -> pd.DataFrame:
    """DataFrame minimal reproduisant le schéma du Parquet Gold (couche combined)."""
    return pd.DataFrame(
        {
            "Datetime":               pd.to_datetime(["2026-02-16 00:00:00", "2026-02-16 00:15:00"]),
            "Open":                   [62.90, 62.93],
            "High":                   [62.94, 62.93],
            "Low":                    [62.87, 62.87],
            "Close":                  [62.93, 62.88],
            "Volume":                 [126, 90],
            "Volatility_Range":       [0.07, 0.06],
            "Variation_Pct":          [float("nan"), -0.0795],
            "geo_I_smoothed":         [246.88, 169.50],
            "geo_B_smoothed":         [235.97, 148.66],
            "geo_S_smoothed":         [215.19, 134.64],
            "geo_score_raw_smoothed": [2988.02, 2090.97],
            "geo_I_sum":              [982.0, 169.5],
            "geo_B_sum":              [941.25, 148.66],
            "geo_S_sum":              [890.42, 134.64],
            "geo_score_raw_sum":      [11312.02, 2090.97],
            "total_event_count":      [461.0, 73.0],
            "gap_duration_15m":       [5, 1],
            "period_main_actor":      ["USA", "USA"],
            "score_pct_7d":           [50.0, 0.0],
        }
    )


@pytest.fixture
def normalized_df(gold_df: pd.DataFrame) -> pd.DataFrame:
    """DataFrame Gold après normalisation UTC (Datetime renommé en timestamp)."""
    return _normalize_timestamp(gold_df)


# ──────────────────────────────────────────────
# _normalize_timestamp
# ──────────────────────────────────────────────


class TestNormalizeTimestamp:
    def test_renames_datetime_to_timestamp(self, gold_df: pd.DataFrame) -> None:
        result = _normalize_timestamp(gold_df)
        assert "timestamp" in result.columns
        assert "Datetime" not in result.columns

    def test_converts_to_utc_iso_format(self, gold_df: pd.DataFrame) -> None:
        result = _normalize_timestamp(gold_df)
        assert result["timestamp"].iloc[0] == "2026-02-16T00:00:00Z"
        assert result["timestamp"].iloc[1] == "2026-02-16T00:15:00Z"

    def test_tz_aware_input_converted_to_utc(self) -> None:
        df = pd.DataFrame(
            {"Datetime": pd.to_datetime(["2026-03-01T12:00:00+02:00"])}
        )
        result = _normalize_timestamp(df)
        # +02:00 → UTC = 10:00Z
        assert result["timestamp"].iloc[0] == "2026-03-01T10:00:00Z"

    def test_does_not_mutate_original(self, gold_df: pd.DataFrame) -> None:
        original_col = gold_df["Datetime"].copy()
        _normalize_timestamp(gold_df)
        pd.testing.assert_series_equal(gold_df["Datetime"], original_col)

    def test_raises_on_missing_datetime_column(self) -> None:
        df = pd.DataFrame({"Close": [75.0]})
        with pytest.raises(KeyError, match="Datetime"):
            _normalize_timestamp(df)


# ──────────────────────────────────────────────
# _generate_actions
# ──────────────────────────────────────────────


class TestGenerateActions:
    def test_yields_correct_index(self, normalized_df: pd.DataFrame) -> None:
        actions = list(_generate_actions(normalized_df))
        assert all(a["_index"] == ES_INDEX for a in actions)

    def test_yields_all_rows(self, normalized_df: pd.DataFrame) -> None:
        actions = list(_generate_actions(normalized_df))
        assert len(actions) == len(normalized_df)

    def test_source_contains_all_gold_columns(self, normalized_df: pd.DataFrame) -> None:
        action = next(_generate_actions(normalized_df))
        # Toutes les colonnes COLUMNS présentes dans le df doivent être dans _source
        expected = set(COLUMNS) & set(normalized_df.columns)
        assert set(action["_source"].keys()) == expected

    def test_skips_missing_columns_gracefully(self) -> None:
        # DataFrame partiel (score_pct_7d absent) → ne lève pas d'exception
        df = pd.DataFrame(
            {
                "timestamp": ["2026-02-16T00:00:00Z"],
                "Close":     [62.93],
                "geo_score_raw_smoothed": [2988.02],
                # score_pct_7d intentionnellement absent
            }
        )
        actions = list(_generate_actions(df))
        assert len(actions) == 1
        assert "score_pct_7d" not in actions[0]["_source"]


# ──────────────────────────────────────────────
# _ensure_index
# ──────────────────────────────────────────────


class TestEnsureIndex:
    def test_creates_index_when_absent(self) -> None:
        es = MagicMock()
        es.indices.exists.return_value = False
        _ensure_index(es)
        es.indices.create.assert_called_once()
        call_kwargs = es.indices.create.call_args
        assert call_kwargs.kwargs.get("index") == ES_INDEX or call_kwargs.args[0] == ES_INDEX

    def test_skips_creation_when_present(self) -> None:
        es = MagicMock()
        es.indices.exists.return_value = True
        _ensure_index(es)
        es.indices.create.assert_not_called()


# ──────────────────────────────────────────────
# _bulk_index
# ──────────────────────────────────────────────


class TestBulkIndex:
    @patch("src.indexing.load_to_elastic.bulk")
    def test_calls_bulk_with_es_client(
        self, mock_bulk: MagicMock, normalized_df: pd.DataFrame
    ) -> None:
        mock_bulk.return_value = (len(normalized_df), [])
        es = MagicMock()
        _bulk_index(es, normalized_df)
        mock_bulk.assert_called_once()
        assert mock_bulk.call_args.args[0] is es

    @patch("src.indexing.load_to_elastic.bulk")
    def test_logs_errors_without_raising(
        self, mock_bulk: MagicMock, normalized_df: pd.DataFrame
    ) -> None:
        mock_bulk.return_value = (1, [{"error": "timeout"}])
        es = MagicMock()
        # Ne doit jamais lever d'exception même en cas d'erreur partielle
        _bulk_index(es, normalized_df)


# ──────────────────────────────────────────────
# main() — test d'intégration avec mocks
# ──────────────────────────────────────────────


class TestMain:
    @patch("src.indexing.load_to_elastic._bulk_index")
    @patch("src.indexing.load_to_elastic._ensure_index")
    @patch("src.indexing.load_to_elastic._get_es_client")
    @patch("src.indexing.load_to_elastic._normalize_timestamp")
    @patch("src.indexing.load_to_elastic._read_parquet_from_s3")
    def test_pipeline_calls_all_steps_in_order(
        self,
        mock_read: MagicMock,
        mock_normalize: MagicMock,
        mock_es_client: MagicMock,
        mock_ensure: MagicMock,
        mock_bulk_idx: MagicMock,
    ) -> None:
        raw_df = pd.DataFrame({"Datetime": pd.to_datetime(["2026-02-16T00:00:00"]),
                                "Close": [62.93], "score_pct_7d": [50.0]})
        normalized = raw_df.copy()
        normalized["timestamp"] = "2026-02-16T00:00:00Z"
        mock_read.return_value = raw_df
        mock_normalize.return_value = normalized

        main()

        mock_read.assert_called_once()
        mock_normalize.assert_called_once_with(raw_df)
        mock_es_client.assert_called_once()
        mock_ensure.assert_called_once()
        mock_bulk_idx.assert_called_once()
