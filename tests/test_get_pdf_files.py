import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from io import BytesIO
import aggregate_pdf_reports  # replace with your actual module
import os
# import time # No need to import time directly in the test if patching from aggregate_pdf_reports

# Define MAX_RETRIES for the test environment, as it's used in the function
aggregate_pdf_reports.MAX_RETRIES = 3

class AttrDict:
    def __init__(self, d):
        self.__dict__.update(d)

    def __contains__(self, key):
        return hasattr(self, key)

# Helper fake DICOM object with dict-like access
class FakeDicom:
    def __init__(self, attrs):
        self.attrs = attrs
    def get(self, key, default=None):
        return self.attrs.get(key, default)

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_success(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: Successful retrieval of PDF file names

    # Setup fake DB result rows
    fake_db_rows = [
        ("file1.dcm", "pdf-reports"),
        ("file2.dcm", "pdf-reports"),
    ]

    # Setup mock psycopg connection and cursor
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fake_db_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    # Setup fake blob client and blob_service_client
    mock_blob_client = MagicMock()
    # This will return bytes of a fake DICOM file
    mock_blob_client.download_blob.return_value.readall.return_value = b"FAKEDICOMDATA"
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    # Setup fake dicom file returned by pydicom.dcmread
    def fake_dcmread(fileobj):
        return FakeDicom({
            "InstanceCreationDate": "20250115",
            "InstanceCreationTime": "120000",
            "StudyInstanceUID": "1.2.3",
            "SeriesInstanceUID": "4.5.6",
            "ReferencedSeriesSequence": [AttrDict({"SeriesInstanceUID": "4.5.6"})],
            "ReferencedPerformedProcedureStepSequence": [AttrDict({"ReferencedSOPInstanceUID": "7.8.9"})]
        })
    mock_dcmread.side_effect = fake_dcmread

    # Call the function with a date range that includes 2025-01-15 12:00:00
    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    result = aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)

    # It should return filenames constructed from the UIDs
    assert result == ["1.2.3_4.5.6_7.8.9.pdf", "1.2.3_4.5.6_7.8.9.pdf"]

    # Validate the DB was queried with correct params
    mock_cursor.execute.assert_called_once()
    query_params = mock_cursor.execute.call_args[0][1]
    assert query_params["from"] == from_date
    assert query_params["to"] == to_date

    # Validate blob client called for each file
    calls = mock_blob_service_client.from_connection_string.return_value.get_blob_client.call_args_list
    assert calls[0][0] == ("pdf-reports", "file1.dcm")
    assert calls[1][0] == ("pdf-reports", "file2.dcm")

    # Validate dicom read was called twice
    assert mock_dcmread.call_count == 2

@patch("aggregate_pdf_reports.sleep") 
@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_db_unreachable_retries(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv, mock_sleep):
    # Test case: Database unreachable, verify retries and eventual failure
    mock_psycopg_connect.side_effect = Exception("DB Connection Error")

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    with pytest.raises(ValueError, match="Failed to connect to the database after retries."):
        aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)

    # Assert that connect was called MAX_RETRIES times
    assert mock_psycopg_connect.call_count == aggregate_pdf_reports.MAX_RETRIES
    # Assert that sleep was called MAX_RETRIES - 1 times (no sleep after the last failed attempt)
    assert mock_sleep.call_count == aggregate_pdf_reports.MAX_RETRIES - 1


@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_db_query_error(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: Database query fails after connection
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("Query Error")
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    with pytest.raises(ValueError, match="Query Error"):
        aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)

    mock_cursor.execute.assert_called_once()


@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_blob_not_in_storage(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: Blob not found in Azure storage (continue branch)
    fake_db_rows = [
        ("file1.dcm", "pdf-reports"),
    ]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fake_db_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.side_effect = Exception("Blob not found") # Simulate blob not found
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    result = aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)
    assert result == [] # Expect empty list as the file is skipped

    mock_blob_client.download_blob.assert_called_once()
    mock_dcmread.assert_not_called() # pydicom should not be called if blob download fails

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_dicom_read_failed(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: pydicom.dcmread fails (continue branch)
    fake_db_rows = [
        ("file1.dcm", "pdf-reports"),
    ]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fake_db_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"INVALIDDICOMDATA"
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    mock_dcmread.side_effect = Exception("Invalid DICOM file") # Simulate pydicom read failure

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    result = aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)
    assert result == [] # Expect empty list as the file is skipped

    mock_dcmread.assert_called_once()

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_invalid_creation_datetime(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: Invalid InstanceCreationDate/Time in DICOM (continue branch)
    fake_db_rows = [
        ("file1.dcm", "pdf-reports"),
    ]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fake_db_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"FAKEDICOMDATA"
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    def fake_dcmread(fileobj):
        return FakeDicom({
            "InstanceCreationDate": "INVALIDDATE", # Invalid date
            "InstanceCreationTime": "120000",
            "StudyInstanceUID": "1.2.3",
            "SeriesInstanceUID": "4.5.6",
            "ReferencedSeriesSequence": [AttrDict({"SeriesInstanceUID": "4.5.6"})],
            "ReferencedPerformedProcedureStepSequence": [AttrDict({"ReferencedSOPInstanceUID": "7.8.9"})]
        })
    mock_dcmread.side_effect = fake_dcmread

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    result = aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)
    assert result == []

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_date_not_in_range(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: DICOM creation date not within the specified range (continue branch)
    fake_db_rows = [
        ("file1.dcm", "pdf-reports"),
    ]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fake_db_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"FAKEDICOMDATA"
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    def fake_dcmread(fileobj):
        return FakeDicom({
            "InstanceCreationDate": "20240115", # Date outside the range
            "InstanceCreationTime": "120000",
            "StudyInstanceUID": "1.2.3",
            "SeriesInstanceUID": "4.5.6",
            "ReferencedSeriesSequence": [AttrDict({"SeriesInstanceUID": "4.5.6"})],
            "ReferencedPerformedProcedureStepSequence": [AttrDict({"ReferencedSOPInstanceUID": "7.8.9"})]
        })
    mock_dcmread.side_effect = fake_dcmread

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    result = aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)
    assert result == []

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_missing_study_instance_uid(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: Missing StudyInstanceUID (continue branch)
    fake_db_rows = [
        ("file1.dcm", "pdf-reports"),
    ]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fake_db_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"FAKEDICOMDATA"
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    def fake_dcmread(fileobj):
        return FakeDicom({
            "InstanceCreationDate": "20250115",
            "InstanceCreationTime": "120000",
            # "StudyInstanceUID": "1.2.3", # Missing
            "SeriesInstanceUID": "4.5.6",
            "ReferencedSeriesSequence": [AttrDict({"SeriesInstanceUID": "4.5.6"})],
            "ReferencedPerformedProcedureStepSequence": [AttrDict({"ReferencedSOPInstanceUID": "7.8.9"})]
        })
    mock_dcmread.side_effect = fake_dcmread

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    result = aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)
    assert result == []

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_missing_series_instance_uid(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: Missing SeriesInstanceUID (continue branch)
    fake_db_rows = [
        ("file1.dcm", "pdf-reports"),
    ]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fake_db_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"FAKEDICOMDATA"
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    def fake_dcmread(fileobj):
        return FakeDicom({
            "InstanceCreationDate": "20250115",
            "InstanceCreationTime": "120000",
            "StudyInstanceUID": "1.2.3",
            # "SeriesInstanceUID": "4.5.6", # Missing
            "ReferencedSeriesSequence": [], # Empty sequence
            "ReferencedPerformedProcedureStepSequence": [AttrDict({"ReferencedSOPInstanceUID": "7.8.9"})]
        })
    mock_dcmread.side_effect = fake_dcmread

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    result = aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)
    assert result == []

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_missing_referenced_sop_instance_uid(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: Missing ReferencedSOPInstanceUID (continue branch)
    fake_db_rows = [
        ("file1.dcm", "pdf-reports"),
    ]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fake_db_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"FAKEDICOMDATA"
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    def fake_dcmread(fileobj):
        return FakeDicom({
            "InstanceCreationDate": "20250115",
            "InstanceCreationTime": "120000",
            "StudyInstanceUID": "1.2.3",
            "SeriesInstanceUID": "4.5.6",
            "ReferencedSeriesSequence": [AttrDict({"SeriesInstanceUID": "4.5.6"})],
            "ReferencedPerformedProcedureStepSequence": [] # Empty sequence
        })
    mock_dcmread.side_effect = fake_dcmread

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    result = aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)
    assert result == []

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.psycopg.connect")
@patch("aggregate_pdf_reports.BlobServiceClient")
@patch("aggregate_pdf_reports.pydicom.dcmread")
def test_get_pdf_file_names_limit_reached(mock_dcmread, mock_blob_service_client, mock_psycopg_connect, mock_load_dotenv):
    # Test case: Verify that the function breaks after 10 successful files
    fake_db_rows = []
    for i in range(15): # More than 10 files
        fake_db_rows.append((f"file{i+1}.dcm", "pdf-reports"))

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fake_db_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"FAKEDICOMDATA"
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    def fake_dcmread(fileobj):
        return FakeDicom({
            "InstanceCreationDate": "20250115",
            "InstanceCreationTime": "120000",
            "StudyInstanceUID": "1.2.3",
            "SeriesInstanceUID": "4.5.6",
            "ReferencedSeriesSequence": [AttrDict({"SeriesInstanceUID": "4.5.6"})],
            "ReferencedPerformedProcedureStepSequence": [AttrDict({"ReferencedSOPInstanceUID": "7.8.9"})]
        })
    mock_dcmread.side_effect = fake_dcmread

    from_date = datetime(2025, 1, 14)
    to_date = datetime(2025, 1, 16)

    result = aggregate_pdf_reports.get_pdf_file_names(from_date, to_date)

    assert len(result) == 10 # Should only return 10 files
    assert mock_dcmread.call_count == 10 # dcmread should also be called 10 times