"""
The worker side of the queue/worker split. A small number of these
coroutines pull job ids off an asyncio.Queue and process them
independently of any HTTP request.

Non-negotiables live here, explicitly, not as an afterthought:
  - IDEMPOTENCY: if a job id gets processed twice (duplicate enqueue,
    a crash-and-restart replay, etc.) we check its current status
    before doing any work. A SUCCEEDED job is never redone.
  - RETRIES: transient failures get retried with exponential backoff,
    up to MAX_ATTEMPTS, before we give up.
  - ALERTS: exhausting retries calls alert() — replace the body with
    a real Slack/PagerDuty/email call. Silently failing jobs is the
    one outcome this whole design exists to prevent.
"""
import asyncio
import logging

from app.ai_task import call_ai
from app.jobs import JobStatus, store

logger = logging.getLogger("worker")

MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 2  # attempt 1 -> 2s, attempt 2 -> 4s, ...

job_queue: asyncio.Queue[str] = asyncio.Queue()


def alert(job_id: str, error: Exception) -> None:
    """Someone must find out. Wire this to Slack/PagerDuty/email in prod."""
    logger.critical("ALERT: job %s failed permanently after %s attempts: %s",
                     job_id, MAX_ATTEMPTS, error)


async def process_job(job_id: str) -> None:
    job = store.get(job_id)
    if job is None:
        logger.warning("Worker got unknown job id %s", job_id)
        return

    # Idempotency guard: if this job already finished (e.g. it was
    # enqueued twice, or we're replaying after a crash), don't redo
    # the work or clobber a good result.
    if job.status == JobStatus.SUCCEEDED:
        logger.info("Job %s already succeeded, skipping duplicate run", job_id)
        return

    await store.update(job_id, status=JobStatus.RUNNING)

    attempt = job.attempts
    while attempt < MAX_ATTEMPTS:
        attempt += 1
        try:
            result = await call_ai(job.payload)
            await store.update(job_id, status=JobStatus.SUCCEEDED,
                                result=result, attempts=attempt, error=None)
            return
        except Exception as exc:  # noqa: BLE001 - we want to catch and record any failure
            await store.update(job_id, attempts=attempt, error=str(exc))
            logger.warning("Job %s attempt %s/%s failed: %s",
                           job_id, attempt, MAX_ATTEMPTS, exc)
            if attempt >= MAX_ATTEMPTS:
                await store.update(job_id, status=JobStatus.FAILED)
                alert(job_id, exc)
                return
            await asyncio.sleep(BACKOFF_BASE_SECONDS ** attempt)


async def worker_loop(worker_name: str) -> None:
    logger.info("%s started", worker_name)
    while True:
        job_id = await job_queue.get()
        try:
            await process_job(job_id)
        except Exception:
            logger.exception("Unhandled error processing job %s", job_id)
        finally:
            job_queue.task_done()


def start_workers(count: int = 2) -> list[asyncio.Task]:
    return [asyncio.create_task(worker_loop(f"worker-{i}")) for i in range(count)]
