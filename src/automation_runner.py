import json
from pathlib import Path

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

    # Placeholder for Excel processing
    # Real Excel logic will be added later

    output_file = OUTPUT_DIR / f"processed_{file_path.name}"

    with open(output_file, "w") as f:
        f.write("automation result placeholder")

    logger.info(f"Automation output generated: {output_file}")

    result = {
        "output_file": str(output_file)
    }

    return result