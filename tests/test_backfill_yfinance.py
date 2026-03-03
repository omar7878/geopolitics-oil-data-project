"""
test_backfill_yfinance.py
=========================
Tests unitaires pour src/ingestion/backfill_yfinance.py.

Toutes les dépendances réseau (yfinance) et S3 (boto3) sont mockées.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestExtractHistoricalData:
    @patch("src.ingestion.backfill_yfinance.s3")
    @patch("src.ingestion.backfill_yfinance.yf")
    def test_downloads_and_uploads_per_day(self, mock_yf, mock_s3) -> None:
        from src.ingestion.backfill_yfinance import extract_historical_data

        # Simulate yfinance returning 2 rows on same day
        df = pd.DataFrame({
            "Datetime": pd.to_datetime(["2026-01-04 09:30:00", "2026-01-04 09:45:00"]),
            "Open": [62.90, 62.95],
            "High": [63.00, 63.05],
            "Low": [62.80, 62.85],
            "Close": [62.93, 62.98],
            "Volume": [120, 95],
        })
        df = df.set_index("Datetime")
        # yfinance returns MultiIndex columns for single ticker
        df.columns = pd.MultiIndex.from_tuples(
            [(c, "CL=F") for c in df.columns]
        )
        mock_yf.download.return_value = df

        extract_historical_data(start_date=datetime(2026, 1, 4, 0, 0, tzinfo=timezone.utc))

        mock_yf.download.assert_called_once()
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args
        assert call_kwargs.kwargs["Bucket"] == "datalake"
        assert "raw/yahoofinance/history/2026-01-04" in call_kwargs.kwargs["Key"]

    @patch("src.ingestion.backfill_yfinance.s3")
    @patch("src.ingestion.backfill_yfinance.yf")
    def test_returns_early_when_empty(self, mock_yf, mock_s3) -> None:
        from src.ingestion.backfill_yfinance import extract_historical_data

        mock_yf.download.return_value = pd.DataFrame()

        extract_historical_data(start_date=datetime(2026, 1, 4, 0, 0, tzinfo=timezone.utc))
        mock_s3.put_object.assert_not_called()

    @patch("src.ingestion.backfill_yfinance.s3")
    @patch("src.ingestion.backfill_yfinance.yf")
    def test_uploads_multiple_days(self, mock_yf, mock_s3) -> None:
        from src.ingestion.backfill_yfinance import extract_historical_data

        df = pd.DataFrame({
            "Datetime": pd.to_datetime([
                "2026-01-04 09:30:00",
                "2026-01-05 09:30:00",
            ]),
            "Open": [62.90, 63.10],
            "High": [63.00, 63.20],
            "Low": [62.80, 63.00],
            "Close": [62.93, 63.15],
            "Volume": [120, 200],
        })
        df = df.set_index("Datetime")
        df.columns = pd.MultiIndex.from_tuples(
            [(c, "CL=F") for c in df.columns]
        )
        mock_yf.download.return_value = df

        extract_historical_data(start_date=datetime(2026, 1, 4, 0, 0, tzinfo=timezone.utc))
        assert mock_s3.put_object.call_count == 2

    @patch("src.ingestion.backfill_yfinance.s3")
    @patch("src.ingestion.backfill_yfinance.yf")
    def test_single_level_columns(self, mock_yf, mock_s3) -> None:
        """Test when yfinance returns single-level columns (nlevels == 1)."""
        from src.ingestion.backfill_yfinance import extract_historical_data

        df = pd.DataFrame({
            "Datetime": pd.to_datetime(["2026-01-04 09:30:00"]),
            "Open": [62.90],
            "High": [63.00],
            "Low": [62.80],
            "Close": [62.93],
            "Volume": [120],
        })
        df = df.set_index("Datetime")
        # Single level columns (no MultiIndex)
        mock_yf.download.return_value = df

        extract_historical_data(start_date=datetime(2026, 1, 4, 0, 0, tzinfo=timezone.utc))
        mock_s3.put_object.assert_called_once()
