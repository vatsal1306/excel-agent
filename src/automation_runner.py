import os
import json
import shutil
import uuid
import datetime as dt
import pandas as pd
import src

from pathlib import Path

import src.transformations as T

from src.Logging import logger
from src import OUTPUT_ROOT


def run_excel_automation(job):
    """
    Executes automation logic on the downloaded Excel file.
    Uses a temporary run directory to prevent partial outputs.
    """

    job_id = job["id"]

    # ------------------ TEMP RUN DIRECTORY ------------------
    run_id = f"job_{job_id}_{uuid.uuid4().hex[:6]}"
    run_dir = Path(OUTPUT_ROOT) / "_runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Using temporary run directory: {run_dir}")

    # Override OUTPUT_ROOT for transformations
    original_output_root = src.OUTPUT_ROOT
    src.OUTPUT_ROOT = str(run_dir)

    try:
        # ------------------ JOB PAYLOAD ------------------
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

        # ------------------ LOAD EXCEL ------------------
        df = pd.read_excel(file_path, engine="openpyxl")

        # ------------------ STEP 1 ------------------
        logger.info("Running transformation STEP 1")
        df = T.step_01(df, save=True)

        # ------------------ STEP 2 ------------------
        logger.info("Running transformation STEP 2")
        wb = T.step_02(
            file_in=df,
            sheet_name=None,
            header_scan_rows=20,
            keep_net_value_blanks=True,
            save=True
        )

        # ------------------ STEP 3 ------------------
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

        # ------------------ STEP 4 ------------------
        logger.info("Running transformation STEP 4")

        wb = T.step_04_create_distribution_tabs(
            wb,
            source_sheet_name=None,
            header_scan_rows=20,
            save=True,
            save_name="step4_distribution_tabs.xlsx",
        )

        # ------------------ STEP 5 ------------------
        logger.info("Running transformation STEP 5")

        wb = T.step_05_create_orders_on_hold_tabs(
            wb,
            source_sheet_name="Sheet1",
            header_scan_rows=20,
            save=True,
            save_name="step5_orders_on_hold.xlsx",
        )

        # ------------------ STEP 6 ------------------
        logger.info("Running transformation STEP 6")

        wb = T.step_06_create_contractor_tabs(
            wb,
            min_lines=4,
            header_scan_rows=20,
            save=True,
            save_name="step6_contractor_tabs.xlsx",
        )

        # ------------------ STEP 7 (PDF EXPORT) ------------------
        logger.info("Running transformation STEP 7")

        pdf_files = T.step_07_export_tabs_to_pdfs(
            workbook_path=os.path.join(run_dir, "step6_contractor_tabs.xlsx"),
            output_dir=os.path.join(run_dir, "pdf_exports"),
            report_date=dt.date.today(),
            exclude_sheets=["Sheet1"]
        )

        # ------------------ MOVE FINAL OUTPUT ------------------
        final_output_dir = Path(original_output_root)
        final_output_dir.mkdir(parents=True, exist_ok=True)

        final_excel = final_output_dir / "step6_contractor_tabs.xlsx"
        shutil.move(run_dir / "step6_contractor_tabs.xlsx", final_excel)

        # Move PDFs
        pdf_src = run_dir / "pdf_exports"
        pdf_dst = final_output_dir / "pdf_exports"
        pdf_dst.mkdir(parents=True, exist_ok=True)

        if pdf_src.exists():
            for pdf in pdf_src.glob("*.pdf"):
                shutil.move(str(pdf), pdf_dst / pdf.name)

        logger.info(f"Automation output generated: {final_excel}")

        result = {
            "output_file": str(final_excel),
            "pdf_files": [str(p) for p in pdf_dst.glob("*.pdf")]
        }

        return result

    finally:
        # Restore OUTPUT_ROOT
        src.OUTPUT_ROOT = original_output_root

        # Cleanup temporary run directory
        shutil.rmtree(run_dir, ignore_errors=True)