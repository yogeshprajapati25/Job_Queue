# Distributed Job Queue System

[![Live Demo](https://img.shields.io/badge/demo-live-brightgreen)](https://job-queue-api-wcg6.onrender.com)
[![Python](https://img.shields.io/badge/python-3.11-blue)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110.0-009688)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED)](https://www.docker.com/)

A production-grade asynchronous job queue system that decouples heavy background tasks from HTTP request handling using FastAPI, RabbitMQ, and PostgreSQL. Prevents API thread starvation and ensures reliable task execution with exponential backoff retry and dead-letter handling.

**[🚀 Live Demo](https://job-queue-api-wcg6.onrender.com)** | **[📖 API Docs](https://job-queue-api-wcg6.onrender.com/docs)**

---

## Architecture

```mermaid
graph LR
    A[Client] -->|POST /jobs| B[FastAPI API]
    B -->|1. Persist PENDING| C[(PostgreSQL)]
    B -->|2. Publish message| D[RabbitMQ Queue]
    D -->|Consume| E[Background Worker]
    E -->|Execute handler| F[Job Logic]
    F -->|Update status| C
    E -->|Retry on failure| D
    
    style B fill:#4CAF50
    style D fill:#FF9800
    style E fill:#2196F3
    style C fill:#9C27B0
```

**Flow:**
1. API accepts job → returns `202 Accepted` in <100ms
2. Message published to durable RabbitMQ queue
3. Worker consumes message independently
4. On failure: retry with exponential backoff (2s → 4s → 8s)
5. After 3 retries: mark as `DEAD_LETTER`

---

## Features

- ✅ **Async Processing** — API never blocks, heavy tasks run in background
- ✅ **Retry Logic** — Exponential backoff with configurable max retries
- ✅ **Dead Letter Queue** — Failed jobs tracked in database
- ✅ **API Key Authentication** — Secure endpoints with header-based auth
- ✅ **Health Monitoring** — `/health` endpoint checks DB + RabbitMQ connectivity
- ✅ **Structured Logging** — JSON logs with job_id, duration, status
- ✅ **Job Cancellation** — Cancel pending jobs before worker picks them up
- ✅ **Horizontal Scaling** — Run multiple worker replicas with fair dispatch
- ✅ **Database Migrations** — Alembic for schema versioning
- ✅ **Deployed & Live** — Production deployment on Render

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Web API | FastAPI + Uvicorn |
| Message Broker | RabbitMQ (CloudAMQP) |
| Database | PostgreSQL + SQLAlchemy |
| Migrations | Alembic |
| Deployment | Render (Docker) |
| Language | Python 3.11 |

---

## Quick Start

### 1. Clone & Setup

```bash
git clone https://github.com/yogeshprajapati25/Job_Queue.git
cd Job_Queue
cp .env.example .env
# Edit .env with your DATABASE_URL, RABBITMQ_URL, API_KEY
```

### 2. Run with Docker Compose

```bash
docker compose up --build
```

Services start on:
- API: `http://localhost:8000`
- API Docs: `http://localhost:8000/docs`
- RabbitMQ Management: `http://localhost:15673`

### 3. Test the API

**Health Check:**
```bash
curl http://localhost:8000/health
```

**Create a Job:**
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{"job_type": "send_email", "payload": {"to": "user@example.com"}}'
```

**Check Job Status:**
```bash
curl http://localhost:8000/jobs/{job_id} \
  -H "X-API-Key: your-api-key"
```

---

## API Endpoints

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| `GET` | `/` | Frontend dashboard | No |
| `GET` | `/health` | System health check | No |
| `POST` | `/jobs` | Submit new job | Yes |
| `GET` | `/jobs` | List all jobs (paginated) | Yes |
| `GET` | `/jobs/{id}` | Get job status | Yes |
| `DELETE` | `/jobs/{id}` | Cancel pending job | Yes |

---

## Job Types

| Type | Simulates | Duration |
|------|-----------|----------|
| `send_email` | Email delivery | 2s |
| `generate_report` | PDF generation | 30s |
| `resize_image` | Image processing | 1s |

---

## Job Lifecycle

```
PENDING → PROCESSING → COMPLETED ✅
                    ↘ FAILED (retry 1) → FAILED (retry 2) → FAILED (retry 3) → DEAD_LETTER 💀
PENDING → CANCELLED (via DELETE /jobs/{id})
```

---

## Scaling Workers

Run multiple worker instances for parallel processing:

```bash
docker compose up --scale worker=3
```

RabbitMQ distributes messages fairly across workers using `prefetch_count=1`.

---

## Environment Variables

```env
DATABASE_URL=postgresql://user:pass@host:5432/dbname
RABBITMQ_URL=amqp://user:pass@host:5672/
API_KEY=your-secret-api-key-here
```

Generate a secure API key:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

---

## Project Structure

```
Job_Queue/
├── app/
│   ├── main.py          # FastAPI routes
│   ├── models.py        # SQLAlchemy models
│   ├── database.py      # DB connection
│   ├── producer.py      # RabbitMQ publisher
│   ├── auth.py          # API key validation
│   ├── logger.py        # Structured logging
│   └── static/
│       └── index.html   # Frontend dashboard
├── worker/
│   └── consumer.py      # Background job processor
├── alembic/             # Database migrations
├── docker-compose.yml
├── Dockerfile.api
├── Dockerfile.worker
├── requirements.txt
└── README.md
```

---

## Deployment

Deployed on [Render](https://render.com) with:
- 2 web services (API + Worker with health endpoint)
- 1 PostgreSQL database
- CloudAMQP for hosted RabbitMQ

Auto-deploys on push to `main` branch.

---

## Development

**Run migrations:**
```bash
docker compose exec api alembic upgrade head
```

**Create new migration:**
```bash
docker compose exec api alembic revision --autogenerate -m "description"
```

**View logs:**
```bash
docker compose logs -f worker
```

---

## Future Enhancements

- [ ] WebSocket for real-time job status updates
- [ ] Prometheus metrics endpoint
- [ ] Job priority queues (high/medium/low)
- [ ] Rate limiting per API key
- [ ] S3 integration for file uploads
- [ ] Scheduled/cron jobs

---

## License

MIT

---

## Author

**Yogesh Prajapati**  
[GitHub](https://github.com/yogeshprajapati25) • [LinkedIn](https://linkedin.com/in/your-profile) • [Email](mailto:yogeshparjapati46@gmail.com)
