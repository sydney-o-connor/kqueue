# kqueue

**A PostgreSQL-backed distributed task queue for the JVM, built in Kotlin.**

kqueue is a production-oriented task queue designed for applications that already depend on PostgreSQL and want reliable background job processing without operating another infrastructure service.

It provides concurrent workers, scheduled jobs, automatic retries, dead-letter handling, graceful recovery from crashed workers, and first-class Prometheus/Grafana observability.

> **Design philosophy:** prefer operational simplicity and correctness when PostgreSQL is already part of the stack.

## Why kqueue?

Task queues often introduce another stateful service into an application's infrastructure. kqueue takes a different approach: use PostgreSQL as the durable source of truth and build the queue around transactional database primitives.

This makes kqueue a good fit for workloads where:

* jobs must survive process crashes
* delivery semantics matter more than extreme throughput
* queue state should be inspectable with standard SQL
* the application already operates PostgreSQL
* adding Redis or another queueing system would add unnecessary operational complexity

kqueue uses PostgreSQL row locking with `FOR UPDATE SKIP LOCKED` for safe concurrent job acquisition, avoiding advisory locks and reducing contention between workers.

## Features

* **At-least-once delivery** backed by PostgreSQL
* **Concurrent workers** with configurable worker pools
* **Scheduled and delayed jobs** through `executeAt`
* **Automatic retries** with exponential backoff and jitter
* **Dead-letter queue** for jobs that exhaust their retry attempts
* **Dead-letter recovery** through the REST API and CLI
* **Stale-job recovery** through a background reaper
* **Recurring jobs** through cron-style schedules
* **Graceful worker shutdown**
* **Prometheus metrics** through Micrometer
* **Preconfigured Grafana dashboard**
* **REST API** for queue inspection and job management
* **Kotlin client API** for application integration
* **Integration tests** against real PostgreSQL using Testcontainers

## Architecture

At a high level, kqueue consists of a durable PostgreSQL queue, worker processes, and an HTTP/API layer:

```text
                    ┌─────────────────┐
                    │   Application   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Kqueue Client  │
                    └────────┬────────┘
                             │ enqueue
                             ▼
                 ┌───────────────────────┐
                 │      PostgreSQL       │
                 │                       │
                 │  pending → running    │
                 │      ↓        ↓       │
                 │ completed   failed    │
                 │                 ↓     │
                 │               dead    │
                 └──────────┬────────────┘
                            │
                   SKIP LOCKED dequeue
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
       ┌─────────────┐             ┌─────────────┐
       │   Worker 1  │             │   Worker N  │
       └─────────────┘             └─────────────┘
              │                           │
              └─────────────┬─────────────┘
                            ▼
                    ┌─────────────────┐
                    │   Application   │
                    │    handlers     │
                    └─────────────────┘

             ┌──────────────────────────────┐
             │ Micrometer → Prometheus      │
             │              → Grafana        │
             └──────────────────────────────┘
```

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the detailed design, dequeue strategy, retry behavior, stale-job recovery, and deliberately excluded features.

## Job lifecycle

Jobs move through a small, explicit state machine:

```text
PENDING ──► RUNNING ──► COMPLETED
               │
               └──────► FAILED
                           │
                    retry with backoff
                           │
                           ▼
                         PENDING

FAILED ──► DEAD
          when maximum attempts are exhausted
```

If a worker crashes while processing a job, the job can remain in `RUNNING`. A background reaper detects stale jobs and returns them to `PENDING` so they can be processed again.

## Enqueue a job

The Kotlin client provides a small API for creating jobs:

```kotlin
client.enqueue("notifications") {
    payload = PushNotification(
        userId = 123,
        message = "Your order shipped"
    )
}
```

Jobs can specify execution options:

```kotlin
client.enqueue("reports") {
    payload = ReportRequest(reportId = "q3-summary")
    priority = 10
    maxAttempts = 3
    executeAt = Instant.now().plus(Duration.ofHours(1))
}
```

## Define a worker

Workers process jobs concurrently using Kotlin coroutines:

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

## Scheduled jobs

Recurring jobs can be registered with a cron expression:

```kotlin
scheduler.register(
    name = "daily-digest",
    cron = "0 8 * * *",
    queue = "emails"
) {
    DigestEmailRequest(date = LocalDate.now())
}
```

## Dead-letter handling

Jobs that exhaust their retry attempts move to `DEAD`.

They can then be inspected or recovered through the API:

```bash
# Inspect queue statistics
curl http://localhost:8080/queues/emails/stats

# Retry a dead job
curl -X POST \
  http://localhost:8080/queues/emails/jobs/{id}/retry

# Drain the dead-letter queue
curl -X DELETE \
  http://localhost:8080/queues/emails/dead
```

This makes failures observable rather than silently disappearing into worker logs.

## Observability

kqueue exposes application and queue metrics through Prometheus.

| Metric                        | Description                       |
| ----------------------------- | --------------------------------- |
| `kqueue_jobs_enqueued_total`  | Jobs enqueued by queue            |
| `kqueue_jobs_completed_total` | Successfully completed jobs       |
| `kqueue_jobs_failed_total`    | Failed jobs that will be retried  |
| `kqueue_jobs_dead_total`      | Jobs that exhausted their retries |
| `kqueue_queue_depth`          | Current pending jobs per queue    |
| `kqueue_job_duration_seconds` | Job execution-time histogram      |
| `kqueue_job_wait_seconds`     | Time between enqueue and pickup   |

A preconfigured Grafana dashboard is included for local development.

## Quick start

### Requirements

* JDK 17+
* PostgreSQL 13+
* Docker

### Start the development environment

```bash
git clone https://github.com/sydney-o-connor/kqueue.git
cd kqueue

docker compose up -d
./gradlew run
```

The server starts on port `8080`.

The Docker Compose environment also provides Prometheus and Grafana.

## Testing

Unit tests:

```bash
./gradlew test
```

Integration tests:

```bash
./gradlew integrationTest
```

Run the complete suite:

```bash
./gradlew test integrationTest
```

Integration tests use Testcontainers to exercise the queue against a real PostgreSQL instance.

## Performance

A local benchmark on a single worker node produced the following results with no-op job handlers:

| Concurrency |      Throughput | p50 latency | p99 latency |
| ----------: | --------------: | ----------: | ----------: |
|  10 workers | ~1,200 jobs/sec |        6 ms |       18 ms |
|  50 workers | ~3,800 jobs/sec |        9 ms |       34 ms |
| 100 workers | ~5,100 jobs/sec |       14 ms |       61 ms |

These numbers are workload- and hardware-dependent. Real throughput will depend heavily on handler execution time and PostgreSQL I/O.

kqueue is intentionally not positioned as a replacement for high-throughput Redis-backed queues. Its design prioritizes durability, inspectability, and reducing infrastructure complexity.

## REST API

| Method   | Endpoint                          | Purpose                  |
| -------- | --------------------------------- | ------------------------ |
| `POST`   | `/queues/{queue}/jobs`            | Enqueue a job            |
| `GET`    | `/queues/{queue}/jobs/{id}`       | Get job status           |
| `GET`    | `/queues/{queue}/stats`           | Inspect queue statistics |
| `POST`   | `/queues/{queue}/jobs/{id}/retry` | Retry a dead job         |
| `DELETE` | `/queues/{queue}/dead`            | Drain dead-letter jobs   |
| `GET`    | `/metrics`                        | Prometheus metrics       |
| `GET`    | `/health`                         | Health check             |

See [`docs/api.md`](docs/api.md) for the API reference.

## Technology

* **Kotlin + Coroutines** — application and worker concurrency
* **Ktor** — HTTP server
* **jOOQ** — type-safe SQL
* **HikariCP** — database connection pooling
* **PostgreSQL** — durable queue storage
* **Micrometer + Prometheus** — metrics
* **Grafana** — visualization
* **Testcontainers** — PostgreSQL integration testing
* **Docker Compose** — local infrastructure

## Project structure

```text
kqueue/
├── core/               # queue engine, dequeue logic, retries, reaper
├── worker/             # WorkerPool and coroutine execution
├── scheduler/          # recurring job definitions
├── api/                # Ktor REST API
├── client/             # client SDK
├── metrics/            # Micrometer integration
├── deploy/
│   ├── docker-compose.yml
│   ├── grafana/
│   └── prometheus/
└── docs/
    ├── api.md
    └── runbook.md
```

## Design trade-offs

kqueue deliberately keeps its architecture small.

The PostgreSQL-backed approach provides:

* one less stateful service to operate
* durable queue state
* straightforward inspection and debugging
* transactional database semantics
* familiar operational tooling

The trade-off is that PostgreSQL becomes part of the queue's throughput ceiling. For workloads requiring extremely high sustained throughput, a purpose-built in-memory/distributed queue may be a better fit.

## Roadmap

Potential future work includes:

* Idempotency keys and deduplication
* Batch enqueue APIs
* Per-queue rate limiting
* Web UI for job inspection and dead-letter management
* Pluggable storage backends

## License

MIT
