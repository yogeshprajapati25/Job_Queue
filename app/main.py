from fastapi import FastAPI, Depends, HTTPException, status, Query, Security
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import text
from uuid import UUID
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, ConfigDict
from alembic.config import Config
from alembic import command
import pika
import os

from app.database import get_db, engine, Base
from app.models import Job, JobStatus
from app.producer import publish_job, RABBITMQ_URL
from app.logger import get_logger
from app.auth import verify_api_key
from app.rate_limiter import check_rate_limit

logger = get_logger(__name__)


def run_migrations():
    """Run Alembic migrations on startup instead of create_all."""
    try:
        alembic_cfg = Config("alembic.ini")
        command.upgrade(alembic_cfg, "head")
        logger.info("Database migrations applied successfully")
    except Exception as e:
        logger.error("Failed to apply database migrations", extra={"error": str(e)})
        raise


app = FastAPI(title="Distributed Job Queue API")

# Mount static files directory for frontend
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.on_event("startup")
def startup_event():
    run_migrations()


@app.get("/")
def root():
    """Serve the frontend dashboard"""
    return FileResponse("app/static/index.html")


# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------

class JobCreate(BaseModel):
    job_type: str
    payload: Optional[dict[str, Any]] = None


class JobResponse(BaseModel):
    id: UUID
    job_type: str
    status: str
    payload: Optional[dict[str, Any]] = None
    result: Optional[dict[str, Any]] = None
    error_message: Optional[str] = None
    retry_count: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class JobListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[JobResponse]


class HealthResponse(BaseModel):
    status: str
    database: str
    rabbitmq: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/jobs", response_model=JobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_job(
    job_in: JobCreate,
    db: Session = Depends(get_db),
    api_key: str = Security(verify_api_key),
):
    """
    Accept a job, persist it as PENDING, and enqueue it to RabbitMQ.
    
    Rate limited to 10 requests per 60 seconds per API key to prevent abuse.
    """
    # Check rate limit
    allowed, remaining = check_rate_limit(api_key)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "Rate limit exceeded",
                "message": "Maximum 10 job submissions per minute allowed",
                "retry_after": 60,
            },
            headers={"Retry-After": "60"}
        )
    
    new_job = Job(
        job_type=job_in.job_type,
        payload=job_in.payload,
        status=JobStatus.PENDING,
    )
    db.add(new_job)
    db.commit()
    db.refresh(new_job)

    publish_job(job_id=str(new_job.id), job_type=new_job.job_type)

    logger.info(
        "Job created and enqueued",
        extra={
            "job_id": str(new_job.id), 
            "job_type": new_job.job_type,
            "rate_limit_remaining": remaining
        },
    )
    return new_job


@app.get("/jobs", response_model=JobListResponse)
def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status: PENDING, PROCESSING, COMPLETED, FAILED, DEAD_LETTER"),
    job_type: Optional[str] = Query(None, description="Filter by job type e.g. send_email"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(10, ge=1, le=100, description="Results per page"),
    db: Session = Depends(get_db),
    api_key: str = Security(verify_api_key),
):
    """
    List all jobs with optional filtering by status and job_type.
    Supports pagination via page and page_size query params.
    """
    query = db.query(Job)

    if status:
        # Validate the status value
        valid = [s.value for s in JobStatus]
        if status.upper() not in valid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status '{status}'. Valid values: {valid}"
            )
        query = query.filter(Job.status == status.upper())

    if job_type:
        query = query.filter(Job.job_type == job_type)

    total = query.count()
    items = (
        query
        .order_by(Job.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return JobListResponse(total=total, page=page, page_size=page_size, items=items)


@app.get("/jobs/{job_id}", response_model=JobResponse)
def get_job(
    job_id: UUID,
    db: Session = Depends(get_db),
    api_key: str = Security(verify_api_key),
):
    """Get a single job by its UUID."""
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID {job_id} not found"
        )
    return job


@app.delete("/jobs/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def cancel_job(
    job_id: UUID,
    db: Session = Depends(get_db),
    api_key: str = Security(verify_api_key),
):
    """
    Cancel a PENDING job before the worker picks it up.
    Only PENDING jobs can be cancelled — PROCESSING/COMPLETED cannot be undone.
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID {job_id} not found"
        )
    if job.status != JobStatus.PENDING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot cancel job with status '{job.status}'. Only PENDING jobs can be cancelled."
        )

    # Mark cancelled in DB — the worker will discard the message when it finds
    # the job is no longer PENDING (handle_message already handles missing jobs)
    job.status = JobStatus.CANCELLED
    job.error_message = "Cancelled by user"
    db.commit()

    logger.info("Job cancelled", extra={"job_id": str(job_id)})


@app.get("/health", response_model=HealthResponse)
def health_check(db: Session = Depends(get_db)):
    """
    Check connectivity to PostgreSQL and RabbitMQ.
    Returns 200 if both are reachable, 503 if either is down.
    """
    # Check DB
    db_status = "ok"
    try:
        db.execute(text("SELECT 1"))
    except Exception as e:
        db_status = f"error: {str(e)}"

    # Check RabbitMQ
    rmq_status = "ok"
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        conn = pika.BlockingConnection(params)
        conn.close()
    except Exception as e:
        rmq_status = f"error: {str(e)}"

    overall = "healthy" if db_status == "ok" and rmq_status == "ok" else "degraded"

    if overall == "degraded":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": overall, "database": db_status, "rabbitmq": rmq_status}
        )

    return HealthResponse(status=overall, database=db_status, rabbitmq=rmq_status)
