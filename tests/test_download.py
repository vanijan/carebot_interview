import pytest
from unittest.mock import patch, MagicMock
import aggregate_pdf_reports  # replace with the actual module name where download_pdf_from_azure is defined

def test_download_pdf_success():
    pdf_file_name = "test.pdf"
    expected_bytes = b"%PDF-1.4 some pdf content"

    with patch("aggregate_pdf_reports.BlobServiceClient") as mock_blob_service_client:
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value.readall.return_value = expected_bytes
        mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

        content = aggregate_pdf_reports.download_pdf_from_azure(pdf_file_name)

        mock_blob_service_client.from_connection_string.assert_called_once()
        mock_blob_service_client.from_connection_string.return_value.get_blob_client.assert_called_once_with(
            container="pdf-reports", blob=f"/tmp/{pdf_file_name}"
        )
        mock_blob_client.download_blob.assert_called_once()
        assert content == expected_bytes

def test_download_pdf_failure():
    pdf_file_name = "test.pdf"

    with patch("aggregate_pdf_reports.BlobServiceClient") as mock_blob_service_client:
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.side_effect = Exception("Blob not found")
        mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

        content = aggregate_pdf_reports.download_pdf_from_azure(pdf_file_name)

        assert content == "download_failed"
