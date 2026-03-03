"""
test_backfill_gdelt.py
======================
Tests unitaires pour src/ingestion/backfill_gdelt.py.

Toutes les dépendances réseau (requests) et S3 (boto3) sont mockées.
"""

import io
import zipfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest


class TestRunBackfill:
    @patch("src.ingestion.backfill_gdelt.s3")
    @patch("src.ingestion.backfill_gdelt.requests")
    def test_downloads_and_uploads_parquet(self, mock_requests, mock_s3) -> None:
        from src.ingestion.backfill_gdelt import run_backfill

        # Master file list : 1 entry matching export.CSV.zip after start_date
        master_content = (
            "12345 abc123 http://data.gdeltproject.org/gdeltv2/"
            "20260105093000.export.CSV.zip\n"
        )
        mock_master_resp = MagicMock()
        mock_master_resp.text = master_content
        mock_master_resp.raise_for_status = MagicMock()

        # Create a real zip containing a small CSV
        csv_content = "\t".join(["val"] * 61) + "\n"  # 61 columns (header-less row)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("20260105093000.export.CSV", csv_content)
        zip_buffer.seek(0)

        mock_zip_resp = MagicMock()
        mock_zip_resp.content = zip_buffer.getvalue()

        mock_requests.get.side_effect = [mock_master_resp, mock_zip_resp]

        run_backfill(start_date=datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc))
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args
        assert call_kwargs.kwargs["Bucket"] == "datalake"
        assert "raw/gdelt/history/" in call_kwargs.kwargs["Key"]
        assert call_kwargs.kwargs["Key"].endswith(".parquet")

    @patch("src.ingestion.backfill_gdelt.s3")
    @patch("src.ingestion.backfill_gdelt.requests")
    def test_skips_non_export_files(self, mock_requests, mock_s3) -> None:
        from src.ingestion.backfill_gdelt import run_backfill

        # Master file list with mentions but no export
        master_content = (
            "999 abc http://data.gdeltproject.org/gdeltv2/"
            "20260105093000.mentions.CSV.zip\n"
        )
        mock_master_resp = MagicMock()
        mock_master_resp.text = master_content
        mock_master_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_master_resp

        run_backfill(start_date=datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc))
        mock_s3.put_object.assert_not_called()

    @patch("src.ingestion.backfill_gdelt.s3")
    @patch("src.ingestion.backfill_gdelt.requests")
    def test_handles_error_gracefully(self, mock_requests, mock_s3) -> None:
        from src.ingestion.backfill_gdelt import run_backfill

        mock_requests.get.side_effect = Exception("Network error")

        # Should not raise — prints error instead
        run_backfill(start_date=datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc))
        mock_s3.put_object.assert_not_called()

    @patch("src.ingestion.backfill_gdelt.s3")
    @patch("src.ingestion.backfill_gdelt.requests")
    def test_skips_files_before_start_date(self, mock_requests, mock_s3) -> None:
        from src.ingestion.backfill_gdelt import run_backfill

        # File before start_date
        master_content = (
            "12345 abc123 http://data.gdeltproject.org/gdeltv2/"
            "20260101000000.export.CSV.zip\n"
        )
        mock_master_resp = MagicMock()
        mock_master_resp.text = master_content
        mock_master_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_master_resp

        run_backfill(start_date=datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc))
        mock_s3.put_object.assert_not_called()
