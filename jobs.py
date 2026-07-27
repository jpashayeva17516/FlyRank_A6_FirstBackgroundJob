"""
In-memory job store.

In a real deployment this dict would be a table (Postgres/Redis) so that
job state survives a process restart. The *shape* — status, attempts,
result, error, idempotency_key — stays the same either way.
"""
import asyncio
import time
import uuid
from enum import Enum
from typing import Any, Optional


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Job:
    def __init__(self, payload: dict, idempotency_key: Optional[str] = None):
        self.id: str = str(uuid.uuid4())
        self.payload: dict = payload
        self.idempotency_key: Optional[str] = idempotency_key
        self.status: JobStatus = JobStatus.QUEUED
        self.attempts: int = 0
        self.result: Optional[Any] = None
        self.error: Optional[str] = None
        self.created_at: float = time.time()
        self.updated_at: float = time.time()

    def to_dict(self) -> dict:
        return {
            "job_id": self.id,
            "status": self.status.value,
            "attempts": self.attempts,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class JobStore:
    """Thread-/task-safe store, keyed by job id, with a secondary index
    for idempotency keys so duplicate submissions map back to one job."""

    def __init__(self):
        self._jobs: dict[str, Job] = {}
        self._idempotency_index: dict[str, str] = {}  # idempotency_key -> job_id
        self._lock = asyncio.Lock()

    async def create(self, payload: dict, idempotency_key: Optional[str]) -> tuple[Job, bool]:
        """Returns (job, created). created=False means this idempotency
        key was already seen — the caller must NOT enqueue it again."""
        async with self._lock:
            if idempotency_key and idempotency_key in self._idempotency_index:
                existing = self._jobs[self._idempotency_index[idempotency_key]]
                return existing, False

            job = Job(payload, idempotency_key)
            self._jobs[job.id] = job
            if idempotency_key:
                self._idempotency_index[idempotency_key] = job.id
            return job, True

    def get(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    async def update(self, job_id: str, **fields) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            for k, v in fields.items():
                setattr(job, k, v)
            job.updated_at = time.time()


store = JobStore()
