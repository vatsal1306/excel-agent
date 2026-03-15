import json
import shutil
import uuid
from pathlib import Path

import src

from src.Logging import logger
from src import OUTPUT_ROOT
from src.run_transforms import main as run_transforms


def run_excel_automation(job):
    """
    Execute Excel automation pipeline for a job.

    Uses a temporary run directory so partial outputs are never exposed
    in the main output folder.
    """

    job_id = job["id"]

    # ---------------- TEMP RUN DIRECTORY ----------------
    run_id = f"job_{job_id}_{uuid.uuid4().hex[:6]}"
    run_dir = Path(OUTPUT_ROOT) / "_runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Using temporary run directory: {run_dir}")

    # Temporarily redirect OUTPUT_ROOT
    original_output_root = src.OUTPUT_ROOT
    src.OUTPUT_ROOT = str(run_dir)

    try:

        # ---------------- JOB PAYLOAD ----------------
        input_json = job.get("input_json")

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

        # ---------------- RUN PIPELINE ----------------
        # run_transforms already handles step1–step7 pipeline
        run_transforms(str(file_path))

        # ---------------- LOCATE OUTPUT FILES ----------------
        final_excel = run_dir / "step6_contractor_tabs.xlsx"

        if not final_excel.exists():
            raise Exception("Expected output file not generated")

        pdf_dir = run_dir / "pdf_exports"

        pdf_files = []

        if pdf_dir.exists():
            for pdf in pdf_dir.glob("*.pdf"):
                pdf_files.append(pdf)

        # ---------------- MOVE FINAL OUTPUT ----------------
        final_output_dir = Path(original_output_root)
        final_output_dir.mkdir(parents=True, exist_ok=True)

        final_excel_dst = final_output_dir / final_excel.name
        shutil.move(str(final_excel), final_excel_dst)

        pdf_dst = final_output_dir / "pdf_exports"
        pdf_dst.mkdir(parents=True, exist_ok=True)

        moved_pdfs = []

        for pdf in pdf_files:
            dst = pdf_dst / pdf.name
            shutil.move(str(pdf), dst)
            moved_pdfs.append(str(dst))

        logger.info(f"Automation output generated: {final_excel_dst}")

        return {
            "output_file": str(final_excel_dst),
            "pdf_files": moved_pdfs
        }

    except Exception:
        logger.exception(f"Automation job {job_id} failed")
        raise

    finally:

        # Restore OUTPUT_ROOT
        src.OUTPUT_ROOT = original_output_root

        # Cleanup temporary directory
        shutil.rmtree(run_dir, ignore_errors=True)