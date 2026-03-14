import json
import os
import  datetime as dt
import pandas as pd

from pathlib import Path

import src.transformations as T

from src.Logging import logger
from src.config import DOWNLOAD_DIR, OUTPUT_DIR


def run_excel_automation(job):
    """
    Executes automation logic on the downloaded Excel file.
    """

    job_id = job["id"]
    input_json = job["input_json"]

    if not input_json:
        raise Exception("Job input_json missing")

    payload = json.loads(input_json)

    file_path = payload.get("file_path")

    if not file_path:
        raise Exception("file_path not found in job payload")

    file_path = Path(file_path)

    if not file_path.exists():
        raise Exception(f"Input file not found: {file_path}")

    logger.info(f"Processing Excel file: {file_path}")

    # ===================== LOAD EXCEL =====================
    df = pd.read_excel(file_path)

    # ===================== STEP 1 =====================
    logger.info("Running transformation STEP 1")
    df = T.step_01(df, save=True)

    # ===================== STEP 2 =====================
    logger.info("Running transformation STEP 2")
    wb = T.step_02(
        file_in=df,
        sheet_name=None,
        header_scan_rows=20,
        keep_net_value_blanks=True,
        save=True
    )

    # ===================== STEP 3 =====================
    logger.info("Running transformation STEP 3")

    wb = T.step_03(
        wb,
        "Last G/I Date",
        treat_as_date=True,
        save_name="step3_sorted_by_date.xlsx"
    )

    wb = T.step_03(
        wb,
        "Name 2",
        save_name="step3_sorted_by_name2.xlsx"
    )

    wb = T.step_03(
        wb,
        "Name of ship-to party",
        save_name="step3_sorted_by_shipto.xlsx"
    )

    # ===================== STEP 4 =====================
    logger.info("Running transformation STEP 4")

    wb = T.step_04_create_distribution_tabs(
        wb,
        source_sheet_name=None,
        header_scan_rows=20,
        save=True,
        save_name="step4_distribution_tabs.xlsx",
    )

    # ===================== STEP 5 =====================
    logger.info("Running transformation STEP 5")

    wb = T.step_05_create_orders_on_hold_tabs(
        wb,
        source_sheet_name="Sheet1",
        header_scan_rows=20,
        save=True,
        save_name="step5_orders_on_hold.xlsx",
    )

    # ===================== STEP 6 =====================
    logger.info("Running transformation STEP 6")

    wb = T.step_06_create_contractor_tabs(
        wb,
        min_lines=4,
        header_scan_rows=20,
        save=True,
        save_name="step6_contractor_tabs.xlsx",
    )

    # ===================== STEP 7 (PDF EXPORT) =====================
    logger.info("Running transformation STEP 7")

    pdf_files = T.step_07_export_tabs_to_pdfs(
        workbook_path=os.path.join(OUTPUT_DIR, "step6_contractor_tabs.xlsx"),
        output_dir=os.path.join(OUTPUT_DIR, "pdf_exports"),
        report_date=dt.date.today(),
        exclude_sheets=["Sheet1"]
    )

    output_file = OUTPUT_DIR / "step6_contractor_tabs.xlsx"

    logger.info(f"Automation output generated: {output_file}")

    result = {
        "output_file": str(output_file),
        "pdf_files": pdf_files
    }

    return result