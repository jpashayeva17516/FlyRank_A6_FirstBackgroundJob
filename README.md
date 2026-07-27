# Background Job Demo (BE-06)

Moves a slow AI call out of the request path. The endpoint answers in
milliseconds; an in-process worker does the real work; a status
endpoint reports progress.

## Run it

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Try it

```bash
# Kick off a job — returns instantly with 202
curl -i -X POST http://localhost:8000/analyze \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-1" \
  -d '{"text": "summarize this please"}'

# -> 202 { "job_id": "...", "status": "queued", "status_url": "/jobs/..." }

# Poll status until it flips to succeeded or failed
curl http://localhost:8000/jobs/<job_id>

# Retry the exact same request with the same Idempotency-Key —
# you get back the SAME job_id, not a new job
curl -i -X POST http://localhost:8000/analyze \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-1" \
  -d '{"text": "summarize this please"}'
```

## Where things live

| Piece | File | What it does |
|---|---|---|
| Fast-accept endpoint | `app/main.py` | Creates a job record, enqueues it, returns 202 immediately |
| Status endpoint | `app/main.py` | `GET /jobs/{id}` — reads current state, never blocks |
| Job store | `app/jobs.py` | In-memory now; swap for Postgres/Redis later without changing the interface |
| Worker | `app/worker.py` | Pulls job ids off a queue, runs them, handles retries + alerting |
| Your slow call | `app/ai_task.py` | Replace the body of `call_ai()` with your actual A6 request |

## The three non-negotiables

**Jobs will run twice.**
`process_job()` in `worker.py` checks the job's current status before
doing anything. If it's already `SUCCEEDED`, the second run is a no-op.
On the request side, an `Idempotency-Key` header maps repeat submissions
back to the same job instead of creating a duplicate — `store.create()`
returns `(job, created)` and the endpoint only enqueues when `created`
is `True`.

**Jobs will fail.**
`worker.py` wraps the AI call in a retry loop: up to `MAX_ATTEMPTS`
(3) tries with exponential backoff (2s, 4s...) before giving up. Each
attempt's error is recorded on the job so the status endpoint can show
it.

**Someone must find out.**
When retries are exhausted, `alert()` fires. Right now it just logs at
`CRITICAL`; swap the body for a Slack webhook, PagerDuty call, or email
so a human actually sees it. A job silently sitting at `failed` with
nobody looking at logs is the exact failure mode this pattern exists to
prevent.

## Swapping in a real queue later

This uses `asyncio.Queue` + in-process worker coroutines so there's
zero extra infra to run. If you outgrow a single process (need workers
on a separate machine, or need jobs to survive a crash), the pieces
that change are:
- `job_queue` → a Redis list / BullMQ queue / Celery task
- `store` → a database table instead of a dict
- everything in `main.py` and the retry/alert logic in `worker.py`
  stays conceptually the same
