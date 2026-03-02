"""
test_batch_extract_gdelt.py
===========================
Tests unitaires pour src/ingestion/batch_extract_gdelt.py.

Couverture :
  - _build_timestamps  : génération des 96 timestamps 15-min d'une journée
  - _process_timestamp : téléchargement, parsing CSV, conversion Parquet, upload S3
  - run_gdelt_ingestion : orchestration complète
"""

import io
import zipfile
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.batch_extract_gdelt import (
    GDELT_COLUMNS,
    _build_timestamps,
    _process_timestamp,
    run_gdelt_ingestion,
)


# ──────────────────────────────────────────────
# _build_timestamps
# ──────────────────────────────────────────────


class TestBuildTimestamps:
    def test_returns_96_timestamps(self) -> None:
        ts = _build_timestamps(date(2026, 2, 27))
        assert len(ts) == 96

    def test_first_is_midnight_utc(self) -> None:
        ts = _build_timestamps(date(2026, 2, 27))
        assert ts[0] == datetime(2026, 2, 27, 0, 0, 0, tzinfo=timezone.utc)

    def test_last_is_2345_utc(self) -> None:
        ts = _build_timestamps(date(2026, 2, 27))
        assert ts[-1] == datetime(2026, 2, 27, 23, 45, 0, tzinfo=timezone.utc)

    def test_step_is_15_min(self) -> None:
        ts = _build_timestamps(date(2026, 2, 27))
        deltas = [(ts[i + 1] - ts[i]).seconds for i in range(len(ts) - 1)]
        assert all(d == 900 for d in deltas)  # 900 secondes = 15 min

    def test_all_same_date(self) -> None:
        target = date(2026, 3, 1)
        ts = _build_timestamps(target)
        assert all(t.date() == target for t in ts)


# ──────────────────────────────────────────────
# _process_timestamp
# ──────────────────────────────────────────────


def _make_fake_zip(csv_content: str, filename: str = "20260227000000.export.CSV") -> bytes:
    """Crée un zip en mémoire contenant un CSV GDELT."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, csv_content)
    return buf.getvalue()


class TestProcessTimestamp:
    @patch("src.ingestion.batch_extract_gdelt.s3")
    @patch("src.ingestion.batch_extract_gdelt.requests")
    def test_success_uploads_parquet(self, mock_requests: MagicMock, mock_s3: MagicMock) -> None:
        # Fabriquer un CSV avec 61 colonnes (nombre GDELT v2)
        row = "\t".join(["val"] * len(GDELT_COLUMNS))
        csv_content = row + "\n"

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = _make_fake_zip(csv_content)
        mock_requests.get.return_value = mock_resp
        mock_requests.RequestException = Exception

        ts = datetime(2026, 2, 27, 0, 0, 0, tzinfo=timezone.utc)
        result = _process_timestamp(ts, "2026-02-27")

        assert result is True
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "datalake"
        assert "raw/gdelt/daily/2026-02-27/" in call_kwargs["Key"]
        assert call_kwargs["Key"].endswith(".parquet")

    @patch("src.ingestion.batch_extract_gdelt.s3")
    @patch("src.ingestion.batch_extract_gdelt.requests")
    def test_404_returns_false(self, mock_requests: MagicMock, mock_s3: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_requests.get.return_value = mock_resp
        mock_requests.RequestException = Exception

        ts = datetime(2026, 2, 27, 0, 0, 0, tzinfo=timezone.utc)
        result = _process_timestamp(ts, "2026-02-27")

        assert result is False
        mock_s3.put_object.assert_not_called()

    @patch("src.ingestion.batch_extract_gdelt.s3")
    @patch("src.ingestion.batch_extract_gdelt.requests")
    def test_http_error_returns_false(self, mock_requests: MagicMock, mock_s3: MagicMock) -> None:
        mock_requests.get.side_effect = Exception("Connection timeout")
        mock_requests.RequestException = Exception

        ts = datetime(2026, 2, 27, 0, 0, 0, tzinfo=timezone.utc)
        result = _process_timestamp(ts, "2026-02-27")

        assert result is False


# ──────────────────────────────────────────────
# run_gdelt_ingestion
# ──────────────────────────────────────────────


class TestRunGdeltIngestion:
    @patch("src.ingestion.batch_extract_gdelt._process_timestamp")
    def test_calls_process_for_each_timestamp(self, mock_process: MagicMock) -> None:
        mock_process.return_value = True
        run_gdelt_ingestion(date(2026, 2, 27))
        assert mock_process.call_count == 96

    @patch("src.ingestion.batch_extract_gdelt._process_timestamp")
    def test_handles_mixed_success_and_failure(self, mock_process: MagicMock) -> None:
        # Alterner True/False ne doit pas lever d'exception
        mock_process.side_effect = [True, False] * 48
        run_gdelt_ingestion(date(2026, 2, 27))
        assert mock_process.call_count == 96
