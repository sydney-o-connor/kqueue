# kqueue — Architecture

## Overview

kqueue is a production-grade distributed task queue built in Kotlin. It is designed for reliable, observable, and horizontally scalable background job processing backed by PostgreSQL.

This document covers the core design decisions, data model, concurrency model, failure handling, and observability strategy. Where a decision had non-obvious tradeoffs, the reasoning is explained.

---

## Design Goals

- **Reliability over throughput** — jobs must never be silently dropped, even under worker crashes or network partitions.
- **Operational simplicity** — a single Postgres instance is the only required dependency. No external broker.
- **Observability by default** — queue depth, throughput, latency, and failure rates are first-class concerns, not afterthoughts.
- **Clean API** — the client SDK should be fluent and require minimal boilerplate to enqueue or define a job.

---

## Why Postgres, Not Redis?

This is the most common question. The short answer: Postgres gives us durability and queryability that Redis cannot match without significant additional complexity.

| Concern | Postgres | Redis |
|---|---|---|
| Durability | WAL-backed, ACID | AOF optional, best-effort |
| Concurrent dequeue | `SKIP LOCKED` (safe, performant) | `BRPOPLPUSH` (race-prone without Lua) |
| Dead-letter inspection | SQL queries | Manual key scanning |
| Scheduled jobs | `execute_at` column + index | Sorted sets, no native cron |
| Operational overhead | One dependency you likely already have | Additional service to operate |

The tradeoff is throughput — Redis can handle higher job rates. kqueue is designed for workloads where correctness and operability matter more than raw throughput. For use cases exceeding ~5,000 jobs/sec sustained, a Redis-backed queue is a better fit.

---

## Data Model

All job state lives in a single `jobs` table. This is intentional — joins and separate tables add complexity without meaningful benefit at this scale.

```sql
CREATE TABLE jobs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue         TEXT NOT NULL,
    payload       JSONB NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | RUNNING | COMPLETED | FAILED | DEAD
    priority      INT NOT NULL DEFAULT 0,
    attempts      INT NOT NULL DEFAULT 0,
    max_attempts  INT NOT NULL DEFAULT 3,
    execute_at    TIMESTAMPTZ NOT NULL DEFAULT now(), -- for scheduled/delayed jobs
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    failed_at     TIMESTAMPTZ,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_jobs_dequeue ON jobs (queue, priority DESC, execute_at ASC)
    WHERE status = 'PENDING';
```

The partial index on `(queue, priority DESC, execute_at ASC) WHERE status = 'PENDING'` is critical — it keeps the dequeue query fast as the table grows by only indexing actionable rows.

### Job Lifecycle

```
PENDING ──► RUNNING ──► COMPLETED
                │
                └──► FAILED (retryable)
                         │
                         └──► DEAD (max_attempts exceeded)
```

A job transitions to `RUNNING` when a worker acquires it via `SELECT ... FOR UPDATE SKIP LOCKED`. If the worker crashes mid-execution, a background reaper detects `RUNNING` jobs whose `started_at` exceeds a configurable timeout and resets them to `PENDING`.

---

## Dequeue Strategy: `SKIP LOCKED`

The core of safe concurrent dequeuing is:

```sql
UPDATE jobs
SET status = 'RUNNING', started_at = now(), attempts = attempts + 1
WHERE id = (
    SELECT id FROM jobs
    WHERE queue = :queue
      AND status = 'PENDING'
      AND execute_at <= now()
    ORDER BY priority DESC, execute_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
```

`SKIP LOCKED` means concurrent workers skip rows already locked by another transaction, rather than blocking. This eliminates the thundering herd problem and avoids advisory locks. It requires Postgres 9.5+.

---

## Worker Architecture

Workers are implemented using Kotlin coroutines. Each queue gets a `WorkerPool` with a configurable concurrency level. The pool runs a polling loop that:

1. Attempts to dequeue a job.
2. If a job is found, launches a coroutine to execute it.
3. If no job is found, backs off with a short sleep (configurable, default 500ms).

Concurrency is bounded by a `Semaphore(maxConcurrency)`. This prevents a slow queue from consuming all available coroutines.

```
WorkerPool(queue = "emails", concurrency = 10)
    └── polling loop
            ├── dequeue()  →  coroutine: JobHandler.execute(job)
            ├── dequeue()  →  coroutine: JobHandler.execute(job)
            └── ... (up to 10 concurrent)
```

### Graceful Shutdown

On shutdown signal (SIGTERM), the worker pool:

1. Stops accepting new jobs from the queue.
2. Waits for in-flight coroutines to complete (bounded by `shutdownTimeoutSeconds`).
3. Any jobs still running after the timeout are left in `RUNNING` state — the reaper will recover them.

---

## Retry & Backoff

Failed jobs are retried with exponential backoff and jitter to avoid thundering herd on retries:

```
next_execute_at = now() + baseDelaySeconds * 2^attempts + jitter(0..5s)
```

Default values: `baseDelaySeconds = 5`, `maxAttempts = 3`.

Once `attempts >= max_attempts`, the job transitions to `DEAD` and is written to the dead-letter queue (same table, status = `DEAD`). Dead jobs are never automatically retried — they require explicit operator action via the API or CLI.

---

## Scheduled Jobs

Delayed or scheduled execution is handled via the `execute_at` column. Enqueuing with a future timestamp is sufficient:

```kotlin
client.enqueue("reports") {
    payload = ReportRequest(userId = 42)
    executeAt = Instant.now().plus(Duration.ofHours(1))
}
```

Cron-style recurring jobs are registered at application startup. A lightweight scheduler checks for due recurring job definitions every 10 seconds and enqueues them if no instance is already `PENDING` or `RUNNING` for that definition.

---

## Stale Job Reaper

A background coroutine runs every 60 seconds (configurable) and resets stuck `RUNNING` jobs:

```sql
UPDATE jobs
SET status = 'PENDING', started_at = NULL
WHERE status = 'RUNNING'
  AND started_at < now() - INTERVAL ':timeoutSeconds seconds'
  AND attempts < max_attempts;
```

Jobs exceeding `max_attempts` are moved to `DEAD` instead of `PENDING`.

---

## Observability

Metrics are exported via Micrometer with a Prometheus scrape endpoint at `/metrics`.

| Metric | Type | Labels |
|---|---|---|
| `kqueue_jobs_enqueued_total` | Counter | `queue` |
| `kqueue_jobs_completed_total` | Counter | `queue` |
| `kqueue_jobs_failed_total` | Counter | `queue` |
| `kqueue_jobs_dead_total` | Counter | `queue` |
| `kqueue_queue_depth` | Gauge | `queue` |
| `kqueue_job_duration_seconds` | Histogram | `queue` |
| `kqueue_job_wait_seconds` | Histogram | `queue` (time from enqueue to dequeue) |

A pre-built Grafana dashboard is included in `deploy/grafana/dashboard.json` covering queue depth over time, throughput, failure rate, and p50/p95/p99 job duration per queue.

---

## REST API

The API is built with Ktor. It is intentionally minimal — it exposes operational actions, not a general-purpose query interface.

| Method | Path | Description |
|---|---|---|
| `POST` | `/queues/{queue}/jobs` | Enqueue a job |
| `GET` | `/queues/{queue}/jobs/{id}` | Get job status |
| `GET` | `/queues/{queue}/stats` | Queue depth, throughput, failure rate |
| `POST` | `/queues/{queue}/jobs/{id}/retry` | Re-enqueue a dead job |
| `DELETE` | `/queues/{queue}/dead` | Drain the dead-letter queue |

---

## What Was Deliberately Left Out

- **Multi-tenancy** — not in scope. All queues share a single Postgres schema.
- **Job priorities beyond integer weight** — fair scheduling, quotas, and rate limiting per producer are intentionally deferred.
- **At-most-once delivery** — kqueue is at-least-once. Handlers must be idempotent. A deduplication key mechanism is on the roadmap.
- **Non-Postgres backends** — a pluggable backend interface is defined, but only Postgres is implemented.

---

## Local Development

```bash
docker compose up        # starts Postgres, Prometheus, Grafana
./gradlew run            # starts the kqueue server on :8080
./gradlew test           # unit + integration tests
```

Grafana is available at `http://localhost:3000` (admin/admin). The kqueue dashboard is pre-provisioned.
