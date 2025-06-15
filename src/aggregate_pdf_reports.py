from datetime import datetime, timedelta
from dotenv import load_dotenv
from time import sleep
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from pypdf import PdfWriter
from pathlib import Path

import psycopg
import os
import re
import pydicom
import logging
import argparse



MAX_RETRIES = 5

def get_pdf_file_names(from_: datetime, to: datetime) -> list[str]:
    """
    Retrieve a list of PDF report file names created between `from_` and `to`.
    Filenames are constructed from DICOM metadata, stored in a DB and Azure.
    """
    # query for retriving dcm files
    QUERY = '''SELECT dicom_report.file_name, dicom_report.container_name FROM public.dicom_report
            JOIN dicom_stow_rs ON dicom_report.dicom_stow_rs_id = dicom_stow_rs.id
            WHERE created_at > %(from)s AND created_at <= %(to)s
            ORDER BY dicom_report.id DESC
            '''
    # obtain global variables
    load_dotenv()
    pg_host = os.getenv("PG_HOST")
    pg_user = os.getenv("PG_USER")
    pg_port = os.getenv("PG_PORT")
    pg_database = os.getenv("PG_DATABASE")
    pg_password = os.getenv("PG_PASSWORD")
    connection_string = os.getenv("AZURE_CONNECTION_STRING")

    # retrieve valid dcm files, connection to database with exponential back-off
    result_dcms = []
    for i in range(MAX_RETRIES):
        try:
            with psycopg.connect(user=pg_user, password=pg_password, host=pg_host,
                                 port=pg_port, dbname=pg_database) as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(QUERY, {"from": from_, "to": to})
                        result_dcms = cur.fetchall()
                        break
                    except Exception as e:
                        raise ValueError(e)
        except ValueError as e:
            raise
        except Exception as e:
            print(f"Database unreachable, retrying {i + 1}/{MAX_RETRIES}")
            if i + 1 != MAX_RETRIES:
                sleep(2 ** (i + 1))
    else:
        raise ValueError("Failed to connect to the database after retries.")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # iterate through all the files
    out = []
    for file_name, container in result_dcms:
        print(file_name, end="")

        # Reset variables each iteration
        study_instance_uid = None
        series_instance_uid = None
        referenced_sop_instance_uid = None

        # connect to blob storage
        try:
            blob_client = blob_service_client.get_blob_client(container, file_name)
            content = blob_client.download_blob().readall()
        except Exception:
            print(" not in storage")
            continue
        
        # read the file
        try:
            dicom_file = pydicom.dcmread(BytesIO(content))
        except Exception:
            print(" Read failed")
            continue

        # Parse datetime
        try:
            creation_date = dicom_file.get("InstanceCreationDate", "")
            creation_time = dicom_file.get("InstanceCreationTime", "000000").split('.')[0]
            dt = datetime.strptime(creation_date + creation_time, '%Y%m%d%H%M%S')
        except Exception:
            print(" Invalid creation datetime")
            continue

        # validate time slot
        if not (from_ <= dt < to):
            print(f" Date {dt} not in the range")
            continue

        # Study UID
        study_instance_uid = dicom_file.get("StudyInstanceUID")
        if not study_instance_uid:
            print(" Missing StudyInstanceUID")
            continue

        # Series UID
        series_instance_uid = None
        ref_series_seq = dicom_file.get("ReferencedSeriesSequence", [])
        if ref_series_seq and 'SeriesInstanceUID' in ref_series_seq[0]:
            series_instance_uid = ref_series_seq[0].SeriesInstanceUID
        else:
            series_instance_uid = dicom_file.get("SeriesInstanceUID", None)
        if not series_instance_uid:
            print(" Missing SeriesInstanceUID")
            continue

        # SOP UID
        sop_seq = dicom_file.get("ReferencedPerformedProcedureStepSequence", [])
        if sop_seq and 'ReferencedSOPInstanceUID' in sop_seq[0]:
            referenced_sop_instance_uid = sop_seq[0].ReferencedSOPInstanceUID
        else:
            print(" Missing ReferencedSOPInstanceUID")
            continue

        # All values are present, assemble the file name
        pdf_file = f"{study_instance_uid}_{series_instance_uid}_{referenced_sop_instance_uid}.pdf"
        out.append(pdf_file)
        print(f" file {pdf_file} added")

    return out


def download_pdf_from_azure(pdf_file_name: str) -> bytes:
    """
    This function downloads a PDF report from the Azure Blob Storage stored
    under the given `pdf_file_name`. Uses the 'pdf-reports' container.
    Returns the downloaded PDF report as bytes.
    """
    # Load environment variables
    load_dotenv()
    connection_string = os.getenv("AZURE_CONNECTION_STRING")
    container = "pdf-reports"
    blob = '/tmp/' + pdf_file_name # this was needed in my case

    # Connect to Azure Blob Service
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    try:
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob)

        print(f"Downloading: {blob}")
        content = blob_client.download_blob().readall()
        print(f"✅ Found matching blob: {blob}")
        return content
    except Exception as e:
        print(f"❌ Failed to download blob: {e}")
        return "download_failed"


def store_pdf_on_disk(pdf: bytes) -> str:
    """
    Store the PDF report (received as bytes) on the local file system.
    The target destination is configured via the `PDF_TARGET_DIR` environment variable.
    """
    # if download failed in previous case, return this status instead of relevant string
    if pdf == "download_failed":
        return "download_failed"
    
    # variables for file handling
    load_dotenv()
    save_folder = Path(os.getenv("PDF_TARGET_DIR", "pdf_reports"))
    base_name = "report{}.pdf"
    name_template = re.compile(r"^report(\d+)\.pdf$")
    save_folder.mkdir(parents=True, exist_ok=True)

    # find reportxxx.pdf with highest number
    max_num = 0
    for file in save_folder.iterdir():
        match = name_template.match(file.name)
        if match:
            num = int(match.group(1))
            if num > max_num:
                max_num = num

    # save the pdf file and return the path
    save_path = save_folder / base_name.format(max_num + 1)
    save_path.write_bytes(pdf)

    return str(save_path)


def join_pdfs(pdf_paths: list[str]) -> None:
    """
    Joins multiple PDF files into a single PDF file.
    The output path is configured via the `JOINED_PDF_TARGET_DIR` environment variable.
    """
    # read global variables
    load_dotenv()
    save_folder = Path(os.getenv("JOINED_PDF_TARGET_DIR", "joined_pdfs"))
    save_file_path = save_folder / "joined_report.pdf"
    save_folder.mkdir(parents=True, exist_ok=True)

    # add all relevant pdfs (download_failed are ommited)
    merger = PdfWriter()
    for path_str in pdf_paths:
        if path_str != "download_failed":
            merger.append(Path(path_str))

    # write the joined pdf
    merger.write(str(save_file_path))
    merger.close()

if __name__ == '__main__':
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Download and merge PDFs from Azure by date.")
    parser.add_argument(
        "--date",
        type=str,
        help="Optional start date in format YYYY-MM-DD. If not set, defaults to today.",
    )
    parser.add_argument(
        "--delta",
        type=int,
        default=14,
        help="Number of days forward from the start date (default: 14).",
    )
    args = parser.parse_args()

    # --- Date Handling ---
    if args.date:
        try:
            to_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("⚠️ Invalid date format. Use YYYY-MM-DD.")
    else:
        to_date = datetime.now()

    from_date = to_date - timedelta(days=args.delta)

    print(f"📅 Filtering PDFs from {from_date.date()} to {to_date.date()}")

    # --- Execution ---
    pdf_file_names = get_pdf_file_names(from_=from_date, to=to_date)
    pdf_paths = []
    for pdf_file_name in pdf_file_names:
        pdf = download_pdf_from_azure(pdf_file_name)
        pdf_path = store_pdf_on_disk(pdf)
        pdf_paths.append(pdf_path)

    join_pdfs(pdf_paths)
