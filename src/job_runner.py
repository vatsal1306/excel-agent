import asyncio

from src.Logging import logger
from src.config import POLL_INTERVAL_SECONDS
from src.automation_runner import run_excel_automation
from src.email_dispatcher import dispatch_result_email

from src.database import (
    fetch_pending_jobs,
    mark_job_running,
    mark_job_completed,
    mark_job_failed,
)


async def process_job(job):

    job_id = job["id"]

    try:

        await asyncio.to_thread(mark_job_running, job_id)

        result = await asyncio.to_thread(run_excel_automation, job)

        await asyncio.to_thread(dispatch_result_email, job, result)

        await asyncio.to_thread(mark_job_completed, job_id, result)

        logger.info(f"Job {job_id} completed")

    except Exception as e:

        await asyncio.to_thread(mark_job_failed, job_id, str(e))

        logger.exception(f"Job {job_id} failed")


async def run_batch(jobs):
    """
    Run batch of jobs concurrently
    """
    tasks = [process_job(job) for job in jobs]
    await asyncio.gather(*tasks)


async def start_job_runner():

    logger.info("Job runner started")

    while True:

        jobs = await asyncio.to_thread(fetch_pending_jobs)

        if not jobs:
            logger.info("No pending jobs")

        else:
            await run_batch(jobs)

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def main():
    asyncio.run(start_job_runner())


if __name__ == "__main__":
    main()
