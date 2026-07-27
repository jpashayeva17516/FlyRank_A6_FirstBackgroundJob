"""
POST /analyze   -> 202 instantly, hands off to the background worker
GET  /jobs/{id} -> current status/result of that job

Run with: uvicorn app.main:app --reload
"""
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from app.jobs import store
from app.worker import job_queue, start_workers

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = start_workers(count=2)
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(lifespan=lifespan)


class AnalyzeRequest(BaseModel):
    text: str


@app.post("/analyze", status_code=202)
async def analyze(
    body: AnalyzeRequest,
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    """
    Accepts instantly (202) and returns a job id + status URL.
    The actual AI call happens in the worker, not in this handler.

    Send an Idempotency-Key header if the client might retry the same
    request (flaky network, user double-clicks submit, etc.) — retrying
    with the same key returns the existing job instead of starting a
    second one.
    """
    job, created = await store.create(body.model_dump(), idempotency_key)

    # Only enqueue brand-new jobs — a job returned because of an
    # idempotency-key hit is already queued, running, or done.
    if created:
        await job_queue.put(job.id)

    return {
        "job_id": job.id,
        "status": job.status.value,
        "status_url": f"/jobs/{job.id}",
    }


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    job = store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job.to_dict()
