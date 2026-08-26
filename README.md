# Distributed Job Queue System

A production-grade asynchronous job queue built with **FastAPI**, **RabbitMQ**, **PostgreSQL**, and **Docker**. Decouples heavy background tasks from the HTTP request cycle using a message broker and independent worker processes.

## Architecture

```
┌─────────────┐     POST /jobs      ┌─────────────────┐
│   Client    │ ──────────────────► │   FastAPI API   │
└─────────────┘                     └────────┬────────┘
                                             │ 1. Persist job (PENDING)
                                             │ 2. Publish message
                                             ▼
                                    ┌─────────────────┐
                                    │    RabbitMQ     │
                                    │  (job_queue)    │
                                    └────────┬────────┘
                                             │ Consume message
                                             ▼
                                    ┌─────────────────┐
                                    │  Worker Process │
                                    │  (consumer.py)  │
                                    └────────┬────────┘
                                             │ Update job status
                                             ▼
                                    ┌─────────────────┐
                                    │   PostgreSQL    │
                                    │  (jobs table)   │
                                    └─────────────────┘
```

## Features

- **Async job processing** — API returns immediately (202 Accepted), worker processes in background
- **Multiple job types** — `send_email`, `generate_report`, `resize_image` with distinct handlers
- **Retry with exponential backoff** — failed jobs retry up to 3 times (2s → 4s → 8s delays)
- **Dead-letter handling** — jobs exceeding max retries are marked `DEAD_LETTER` in DB
- **Job cancellation** — cancel `PENDING` jobs before the worker picks them up
- **Structured JSON logging** — every log line is machine-parseable (Datadog/CloudWatch ready)
- **Health check endpoint** — monitors DB and RabbitMQ connectivity
- **Worker scaling** — run multiple worker replicas with fair dispatch via `prefetch_count=1`
- **DB migrations** — Alembic handles schema versioning (no `create_all` in production)

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Web API | FastAPI + Uvicorn |
| Message Broker | RabbitMQ (pika) |
| Database | PostgreSQL + SQLAlchemy ORM |
| Migrations | Alembic |
| Containerization | Docker + Docker Compose |
| Language | Python 3.11 |

## Job Lifecycle

```
PENDING → PROCESSING → COMPLETED
                    ↘ FAILED (retried with backoff)
                         ↘ DEAD_LETTER (max retries exceeded)
PENDING → CANCELLED (via DELETE /jobs/{id})
```

## Quick Start

**1. Clone and configure:**
```bash
git clone <your-repo-url>
cd Job_queue
cp .env.example .env
```

**2. Start all services:**
```bash
docker compose up --build
```

**3. Verify everything is running:**
```bash
curl http://localhost:8000/health
# {"status":"healthy","database":"ok","rabbitmq":"ok"}
```

## API Endpoints

### Create a Job
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_type": "send_email", "payload": {"to": "user@example.com"}}'
```

Response `202 Accepted`:
```json
{
  "id": "802cfc84-c2e5-431a-8dce-eb4e4712d9cf",
  "job_type": "send_email",
  "status": "PENDING",
  "payload": {"to": "user@example.com"},
  "result": null,
  "retry_count": 0,
  "created_at": "2026-08-26T15:07:34"
}
```

### Get Job Status
```bash
curl http://localhost:8000/jobs/802cfc84-c2e5-431a-8dce-eb4e4712d9cf
```

### List Jobs (with filters)
```bash
# All jobs
curl http://localhost:8000/jobs

# Filter by status and paginate
curl "http://localhost:8000/jobs?status=COMPLETED&page=1&page_size=10"

# Filter by job type
curl "http://localhost:8000/jobs?job_type=send_email"
```

### Cancel a Pending Job
```bash
curl -X DELETE http://localhost:8000/jobs/802cfc84-c2e5-431a-8dce-eb4e4712d9cf
# 204 No Content
```

### Health Check
```bash
curl http://localhost:8000/health
# {"status":"healthy","database":"ok","rabbitmq":"ok"}
```

## Supported Job Types

| job_type | Simulates | Payload fields |
|----------|-----------|---------------|
| `send_email` | Email delivery (2s) | `to` |
| `generate_report` | PDF/CSV generation (3s) | `report_id` |
| `resize_image` | Image processing (1s) | `image_id` |

Any other `job_type` falls back to a generic handler.

## Scaling Workers

Run multiple worker replicas — RabbitMQ distributes messages fairly:

```bash
docker compose up --scale worker=3
```

## Interactive API Docs

Swagger UI available at: [http://localhost:8000/docs](http://localhost:8000/docs)

## Database Migrations

Migrations run automatically on API startup via Alembic.

To create a new migration after changing models:
```bash
# Inside the api container
docker compose exec api alembic revision --autogenerate -m "describe your change"
docker compose exec api alembic upgrade head
```

## Project Structure

```
Job_queue/
├── app/
│   ├── main.py          # FastAPI app, routes, startup
│   ├── models.py        # SQLAlchemy Job model + JobStatus enum
│   ├── database.py      # DB engine, session, Base
│   ├── producer.py      # RabbitMQ publisher (connection pooled)
│   └── logger.py        # Structured JSON logger
├── worker/
│   └── consumer.py      # RabbitMQ consumer + job handlers
├── alembic/
│   ├── env.py           # Alembic configuration
│   └── versions/
│       └── 0001_initial_jobs_table.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── alembic.ini
├── .env                 # secrets (git-ignored)
└── .env.example         # template for new contributors
```
