# kqueue

A production-grade distributed task queue for the JVM, written in Kotlin. Backed by PostgreSQL. Built for reliability, observability, and operational simplicity.

```kotlin
val client = KqueueClient.connect("jdbc:postgresql://localhost/mydb")

client.enqueue("emails") {
    payload = SendEmailRequest(to = "user@example.com", template = "welcome")
    maxAttempts = 5
    executeAt = Instant.now().plus(Duration.ofMinutes(2))
}
```

---

## Why kqueue?

Most task queues trade operational simplicity for throughput. kqueue makes the opposite bet: if you're already running Postgres, you don't need another service to operate, monitor, and scale. kqueue gives you:

- **At-least-once delivery** backed by Postgres WAL — jobs survive crashes
- **Safe concurrent dequeuing** via `SELECT ... FOR UPDATE SKIP LOCKED` — no thundering herd, no advisory locks
- **Automatic retries** with exponential backoff and jitter
- **Dead-letter queue** with API-driven retry and drain
- **Scheduled and delayed jobs** via `executeAt`
- **First-class observability** — Micrometer metrics, Prometheus endpoint, and a pre-built Grafana dashboard out of the box

If you need >5,000 jobs/sec sustained throughput, use a Redis-backed queue. If you need correctness, auditability, and one less service to operate, use kqueue.

---

## Quick Start

**Requirements:** JDK 17+, Postgres 13+, Docker (for local dev)

```bash
git clone https://github.com/yourhandle/kqueue
cd kqueue
docker compose up -d          # starts Postgres, Prometheus, Grafana
./gradlew run                  # starts kqueue server on :8080
```

Grafana is available at [http://localhost:3000](http://localhost:3000) (admin / admin) with the kqueue dashboard pre-provisioned.

---

## Features

### Enqueue Jobs

```kotlin
// Simple enqueue
client.enqueue("notifications") {
    payload = PushNotification(userId = 123, message = "Your order shipped")
}

// With options
client.enqueue("reports") {
    payload = ReportRequest(reportId = "q3-summary")
    priority = 10                                         // higher runs first
    maxAttempts = 3                                       // default: 3
    executeAt = Instant.now().plus(Duration.ofHours(1))  // delayed execution
}
```

### Define Workers

```kotlin
val worker = WorkerPool.builder()
    .queue("emails")
    .concurrency(10)
    .handler { job ->
        val request = job.payload<SendEmailRequest>()
        emailService.send(request)
        JobResult.success()
    }
    .build()

worker.start()
```

### Scheduled (Recurring) Jobs

```kotlin
scheduler.register(
    name = "daily-digest",
    cron = "0 8 * * *",
    queue = "emails"
) {
    DigestEmailRequest(date = LocalDate.now())
}
```

### Dead-Letter Queue

Jobs that exhaust all retry attempts move to `DEAD` status. They can be inspected and retried via the REST API or the admin CLI:

```bash
# Inspect dead jobs
curl http://localhost:8080/queues/emails/stats

# Retry a specific dead job
curl -X POST http://localhost:8080/queues/emails/jobs/{id}/retry

# Drain all dead jobs for a queue
curl -X DELETE http://localhost:8080/queues/emails/dead
```

---

## Job Lifecycle

```
PENDING ──► RUNNING ──► COMPLETED
                │
                └──► FAILED (retried with backoff)
                         │
                         └──► DEAD (max attempts exceeded)
```

Stale `RUNNING` jobs (worker crashed mid-execution) are automatically detected and reset to `PENDING` by a background reaper.

---

## Observability

kqueue exposes a Prometheus scrape endpoint at `/metrics` and ships a pre-built Grafana dashboard.

| Metric | Description |
|---|---|
| `kqueue_jobs_enqueued_total` | Jobs enqueued, by queue |
| `kqueue_jobs_completed_total` | Jobs completed successfully |
| `kqueue_jobs_failed_total` | Jobs failed (will retry) |
| `kqueue_jobs_dead_total` | Jobs exhausted all retries |
| `kqueue_queue_depth` | Current pending jobs per queue |
| `kqueue_job_duration_seconds` | Execution time histogram |
| `kqueue_job_wait_seconds` | Time from enqueue to pickup |

![Grafana dashboard screenshot](docs/grafana-dashboard.png)

---

## Configuration

```yaml
kqueue:
  datasource:
    url: jdbc:postgresql://localhost/mydb
    username: kqueue
    password: secret

  worker:
    shutdownTimeoutSeconds: 30
    pollingIntervalMs: 500

  reaper:
    intervalSeconds: 60
    jobTimeoutSeconds: 300   # jobs running longer than this are considered stuck

  retry:
    baseDelaySeconds: 5
    maxJitterSeconds: 5
```

---

## REST API

| Method | Path | Description |
|---|---|---|
| `POST` | `/queues/{queue}/jobs` | Enqueue a job |
| `GET` | `/queues/{queue}/jobs/{id}` | Get job status |
| `GET` | `/queues/{queue}/stats` | Queue depth and throughput |
| `POST` | `/queues/{queue}/jobs/{id}/retry` | Retry a dead job |
| `DELETE` | `/queues/{queue}/dead` | Drain dead-letter queue |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/health` | Health check |

Full API reference: [docs/api.md](docs/api.md)

---

## Stack

- **Kotlin** + Coroutines
- **Ktor** — HTTP server
- **jOOQ** — type-safe SQL (no magic ORM)
- **HikariCP** — connection pooling
- **Micrometer** + Prometheus — metrics
- **Grafana** — dashboards (pre-provisioned via Docker Compose)
- **Postgres 13+** — storage and queue engine
- **Testcontainers** — integration tests against real Postgres

---

## Performance

Benchmarked on a single worker node (4 vCPU, 8GB RAM) against a local Postgres instance:

| Concurrency | Throughput | p50 latency | p99 latency |
|---|---|---|---|
| 10 workers | ~1,200 jobs/sec | 6ms | 18ms |
| 50 workers | ~3,800 jobs/sec | 9ms | 34ms |
| 100 workers | ~5,100 jobs/sec | 14ms | 61ms |

Jobs had no-op handlers. Real throughput depends on handler execution time and Postgres I/O.

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for a full explanation of design decisions, the dequeue strategy, retry logic, the stale job reaper, and what was deliberately left out.

---

## Running Tests

```bash
./gradlew test                  # unit tests
./gradlew integrationTest       # integration tests (requires Docker for Testcontainers)
./gradlew test integrationTest  # full suite
```

---

## Project Structure

```
kqueue/
├── core/               # queue engine, dequeue logic, retry, reaper
├── worker/             # WorkerPool, coroutine execution, graceful shutdown
├── scheduler/          # recurring job definitions
├── api/                # Ktor REST API
├── client/             # SDK for enqueuing jobs from other services
├── metrics/            # Micrometer integration
├── deploy/
│   ├── docker-compose.yml
│   ├── grafana/        # pre-provisioned dashboard
│   └── prometheus/     # scrape config
└── docs/
    ├── api.md
    └── runbook.md
```

---

## Roadmap

- [ ] Idempotency keys / deduplication
- [ ] Batch enqueue API
- [ ] Per-queue rate limiting
- [ ] Web UI for job inspection and dead-letter management
- [ ] Pluggable backend interface (MySQL, CockroachDB)

---

## License

MIT
