import time
import json

from src.Logging import logger
from src.database import get_db_connection
from src.config import POLL_INTERVAL_SECONDS
from src.automation_runner import run_excel_automation
from src.email_dispatcher import dispatch_result_email


def fetch_pending_jobs():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, user_id, inbound_match_id, input_json
        FROM jobs
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 5
    """)

    jobs = cursor.fetchall()
    conn.close()

    return jobs


def mark_job_running(job_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE jobs
        SET status='running', started_at=CURRENT_TIMESTAMP
        WHERE id=?
    """, (job_id,))

    conn.commit()
    conn.close()


def mark_job_completed(job_id, output):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE jobs
        SET status='completed',
            output_json=?,
            finished_at=CURRENT_TIMESTAMP
        WHERE id=?
    """, (json.dumps(output), job_id))

    conn.commit()
    conn.close()


def mark_job_failed(job_id, error_msg):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE jobs
        SET status='failed',
            error_msg=?,
            finished_at=CURRENT_TIMESTAMP
        WHERE id=?
    """, (error_msg, job_id))

    conn.commit()
    conn.close()


def run_automation(job):
    """
    Placeholder automation step.
    Excel automation will be added later.
    """

    logger.info(f"Running automation for job {job['id']}")

    result = {
        "status": "success"
    }

    return result


def process_job(job):

    job_id = job["id"]

    try:

        mark_job_running(job_id)

        result = run_excel_automation(job)

        dispatch_result_email(job, result)

        mark_job_completed(job_id, result)

        logger.info(f"Job {job_id} completed")

    except Exception as e:

        mark_job_failed(job_id, str(e))

        logger.error(f"Job {job_id} failed: {str(e)}")


def start_job_runner():

    logger.info("Job runner started")

    while True:

        jobs = fetch_pending_jobs()

        if not jobs:
            logger.info("No pending jobs")

        for job in jobs:
            process_job(job)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    start_job_runner()