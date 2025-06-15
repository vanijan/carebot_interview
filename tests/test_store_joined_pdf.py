from pathlib import Path
from unittest.mock import patch, MagicMock
from aggregate_pdf_reports import join_pdfs

@patch("aggregate_pdf_reports.load_dotenv")
@patch("aggregate_pdf_reports.os.getenv")
@patch("aggregate_pdf_reports.Path.mkdir")
@patch("aggregate_pdf_reports.PdfWriter")
def test_join_pdfs_skips_failed(mock_writer_class, mock_mkdir, mock_getenv, mock_dotenv):
    mock_getenv.return_value = "/fake/output"
    mock_writer = MagicMock()
    mock_writer_class.return_value = mock_writer

    pdf_files = ["/some/path/valid1.pdf", "download_failed", "/some/path/valid2.pdf"]
    join_pdfs(pdf_files)

    mock_writer.append.assert_any_call(Path("/some/path/valid1.pdf"))
    mock_writer.append.assert_any_call(Path("/some/path/valid2.pdf"))
    assert not any(call.args[0] == "download_failed" for call in mock_writer.append.call_args_list)
    mock_writer.write.assert_called_once_with(str(Path("/fake/output") / "joined_report.pdf"))
    mock_writer.close.assert_called_once()
