from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from uuid import UUID
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, ConfigDict

from app.database import get_db, engine, Base
from app.models import Job, JobStatus
from app.producer import publish_job

# Create database tables automatically
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Distributed Job Queue API")

# --- Pydantic Schemas ---

class JobCreate(BaseModel):
    job_type: str
    payload: Optional[dict[str, Any]] = None

class JobResponse(BaseModel):
    id: UUID
    job_type: str
    status: str
    payload: Optional[dict[str, Any]] = None
    result: Optional[dict[str, Any]] = None   # populated by worker on completion
    error_message: Optional[str] = None
    retry_count: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

# --- API Endpoints ---

@app.post("/jobs", response_model=JobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_job(job_in: JobCreate, db: Session = Depends(get_db)):
    # 1. Persist the job with PENDING status
    new_job = Job(
        job_type=job_in.job_type,
        payload=job_in.payload,
        status=JobStatus.PENDING,   # use enum — no more raw string mismatch
    )
    db.add(new_job)
    db.commit()
    db.refresh(new_job)

    # 2. Publish task message to RabbitMQ
    publish_job(job_id=str(new_job.id), job_type=new_job.job_type)

    return new_job

@app.get("/jobs/{job_id}", response_model=JobResponse)
def get_job(job_id: UUID, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID {job_id} not found"
        )
    return job