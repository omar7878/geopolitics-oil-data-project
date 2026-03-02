"""
test_batch_extract_yfinance.py
==============================
Tests unitaires pour src/ingestion/batch_extract_yfinance.py.

Couverture :
  - extract_daily_data : téléchargement yfinance, nettoyage, upload S3
  - Cas nominal (données disponibles)
  - Cas marché fermé (DataFrame vide)
  - Vérification du path S3 et du format
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.batch_extract_yfinance import extract_daily_data


# ──────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────


def _make_yf_dataframe() -> pd.DataFrame:
    """Simule la sortie de yf.download() avec MultiIndex colonnes."""
    idx = pd.DatetimeIndex(
        ["2026-02-27 09:30:00", "2026-02-27 09:45:00", "2026-02-27 10:00:00"],
        name="Datetime",
    )
    df = pd.DataFrame(
        {
            ("Open", "CL=F"):   [62.90, 62.95, 63.00],
            ("High", "CL=F"):   [62.95, 63.00, 63.05],
            ("Low", "CL=F"):    [62.85, 62.90, 62.95],
            ("Close", "CL=F"):  [62.93, 62.98, 63.02],
            ("Volume", "CL=F"): [120, 95, 110],
        },
        index=idx,
    )
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    return df


# ──────────────────────────────────────────────
# extract_daily_data
# ──────────────────────────────────────────────


class TestExtractDailyData:
    @patch("src.ingestion.batch_extract_yfinance.s3")
    @patch("src.ingestion.batch_extract_yfinance.yf")
    def test_nominal_uploads_parquet(self, mock_yf: MagicMock, mock_s3: MagicMock) -> None:
        mock_yf.download.return_value = _make_yf_dataframe()

        extract_daily_data(date(2026, 2, 27))

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "datalake"
        assert call_kwargs["Key"] == "raw/yahoofinance/daily/2026-02-27/wti_daily.parquet"
        # Le body doit être un bytes (contenu Parquet)
        assert isinstance(call_kwargs["Body"], bytes)
        assert len(call_kwargs["Body"]) > 0

    @patch("src.ingestion.batch_extract_yfinance.s3")
    @patch("src.ingestion.batch_extract_yfinance.yf")
    def test_empty_df_skips_upload(self, mock_yf: MagicMock, mock_s3: MagicMock) -> None:
        """Marché fermé → DataFrame vide → pas d'upload."""
        mock_yf.download.return_value = pd.DataFrame()

        extract_daily_data(date(2026, 2, 27))

        mock_s3.put_object.assert_not_called()

    @patch("src.ingestion.batch_extract_yfinance.s3")
    @patch("src.ingestion.batch_extract_yfinance.yf")
    def test_date_range_passed_to_yfinance(self, mock_yf: MagicMock, mock_s3: MagicMock) -> None:
        mock_yf.download.return_value = pd.DataFrame()

        extract_daily_data(date(2026, 3, 1))

        call_kwargs = mock_yf.download.call_args.kwargs
        assert call_kwargs["start"] == "2026-03-01"
        assert call_kwargs["end"] == "2026-03-02"
        assert call_kwargs["interval"] == "15m"
