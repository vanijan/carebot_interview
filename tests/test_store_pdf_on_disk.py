import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from aggregate_pdf_reports import store_pdf_on_disk

@pytest.fixture
def dummy_pdf():
    return b"%PDF dummy content"

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.os.getenv")
@patch("aggregate_pdf_reports.Path.write_bytes")
@patch("aggregate_pdf_reports.Path.iterdir")
@patch("aggregate_pdf_reports.Path.mkdir")
def test_store_pdf_on_disk_empty_dir(mock_mkdir, mock_iterdir, mock_write, mock_getenv, mock_dotenv, dummy_pdf):
    mock_getenv.return_value = "/fake/dir"
    mock_iterdir.return_value = []
    
    result = store_pdf_on_disk(dummy_pdf)

    expected_path = Path("/fake/dir") / "report1.pdf"
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_write.assert_called_once_with(dummy_pdf)
    assert result == str(expected_path)

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.os.getenv")
@patch("aggregate_pdf_reports.Path.write_bytes")
@patch("aggregate_pdf_reports.Path.iterdir")
@patch("aggregate_pdf_reports.Path.mkdir")
def test_store_pdf_on_disk_with_existing_files(mock_mkdir, mock_iterdir, mock_write, mock_getenv, mock_dotenv, dummy_pdf):
    mock_getenv.return_value = "/fake/dir"
    files = [MagicMock(), MagicMock()]
    files[0].name = "report2.pdf"
    files[1].name = "report5.pdf"
    mock_iterdir.return_value = files

    result = store_pdf_on_disk(dummy_pdf)

    expected_path = Path("/fake/dir") / "report6.pdf"
    assert result == str(expected_path)
    mock_write.assert_called_once_with(dummy_pdf)

def test_store_pdf_on_disk_download_failed():
    assert store_pdf_on_disk("download_failed") == "download_failed"
