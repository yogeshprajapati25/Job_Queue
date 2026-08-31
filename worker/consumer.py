import json
import os
import random
import time
import threading
import pika
from http.server import HTTPServer, BaseHTTPRequestHandler
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Job, JobStatus
from app.logger import get_logger

logger = get_logger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
QUEUE_NAME = "job_queue"
PORT = int(os.getenv("PORT", 8001))


# ---------------------------------------------------------------------------
# Minimal health server so Render (free tier) sees an open port
# ---------------------------------------------------------------------------

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"worker running"}')

    def log_message(self, format, *args):
        pass  # suppress default HTTP access logs


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    logger.info("Worker health server started", extra={"port": PORT})
    server.serve_forever()


# ---------------------------------------------------------------------------
# Job type handlers
# ---------------------------------------------------------------------------

def handle_send_email(payload: dict) -> dict:
    """Simulate sending an email. Returns a result dict stored on the job."""
    time.sleep(2)
    if random.random() < 0.3:
        raise Exception("SMTP connection timed out")
    return {"sent_to": payload.get("to", "unknown"), "message": "Email delivered"}


def handle_generate_report(payload: dict) -> dict:
    """Simulate generating a PDF/CSV report. Takes 30 seconds to show async benefit."""
    time.sleep(30)  # Simulate heavy processing — this is the whole point of async
    if random.random() < 0.3:
        raise Exception("Report generation failed: data source unavailable")
    return {"report_url": f"/reports/{payload.get('report_id', 'unknown')}.pdf"}


def handle_resize_image(payload: dict) -> dict:
    """Simulate image resizing."""
    time.sleep(1)
    if random.random() < 0.3:
        raise Exception("Image processing error: unsupported format")
    return {"resized_url": f"/images/{payload.get('image_id', 'unknown')}_thumb.jpg"}


def handle_generic(payload: dict) -> dict:
    """Fallback handler for unknown job types."""
    time.sleep(2)
    if random.random() < 0.3:
        raise Exception("Transient error in generic handler")
    return {"status": "processed"}


JOB_HANDLERS = {
    "send_email": handle_send_email,
    "generate_report": handle_generate_report,
    "resize_image": handle_resize_image,
}


def process_job(job_type: str, payload: dict) -> dict:
    """Dispatch to the correct handler and return the result."""
    handler = JOB_HANDLERS.get(job_type, handle_generic)
    return handler(payload or {})


# ---------------------------------------------------------------------------
# RabbitMQ message callback
# ---------------------------------------------------------------------------

def handle_message(ch, method, properties, body):
    db: Session = SessionLocal()
    job_id = None
    start_time = time.time()

    try:
        data = json.loads(body)
        job_id = data.get("job_id")

        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            logger.warning("Stale message — job not found, discarding", extra={"job_id": job_id})
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if job.status == JobStatus.CANCELLED:
            logger.info("Job was cancelled — discarding message", extra={"job_id": job_id})
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        logger.info(
            "Job processing started",
            extra={
                "job_id": job_id,
                "job_type": job.job_type,
                "attempt": job.retry_count + 1,
                "max_attempts": job.max_retries + 1,
            },
        )

        job.status = JobStatus.PROCESSING
        db.commit()

        result = process_job(job.job_type, job.payload)

        elapsed = round(time.time() - start_time, 3)
        job.status = JobStatus.COMPLETED
        job.result = result
        job.error_message = None
        db.commit()

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(
            "Job completed",
            extra={
                "job_id": job_id,
                "job_type": job.job_type,
                "duration_seconds": elapsed,
                "result": result,
            },
        )

    except Exception as e:
        db.rollback()
        error_str = str(e)
        logger.error("Job processing failed", extra={"job_id": job_id, "error": error_str})

        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            job.retry_count += 1
            job.error_message = error_str

            if job.retry_count > job.max_retries:
                job.status = JobStatus.DEAD_LETTER
                db.commit()
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.warning(
                    "Job moved to DEAD_LETTER",
                    extra={"job_id": job_id, "retry_count": job.retry_count},
                )
            else:
                job.status = JobStatus.FAILED
                db.commit()

                backoff = 2 ** job.retry_count
                logger.info(
                    "Job will be retried",
                    extra={"job_id": job_id, "attempt": job.retry_count, "backoff_seconds": backoff},
                )
                time.sleep(backoff)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                ch.basic_publish(
                    exchange="",
                    routing_key=QUEUE_NAME,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent
                    ),
                )
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    finally:
        db.close()


# ---------------------------------------------------------------------------
# Worker entry point
# ---------------------------------------------------------------------------

def start_consumer():
    # Start health server in background thread so Render sees an open port
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()

    # Connect to RabbitMQ with retry
    # heartbeat=60 keeps the connection alive on CloudAMQP free tier
    # blocked_connection_timeout=300 prevents silent hangs
    params = pika.URLParameters(RABBITMQ_URL)
    params.heartbeat = 60
    params.blocked_connection_timeout = 300
    connection = None
    retries = 0
    max_retries = 10

    while retries < max_retries:
        try:
            logger.info("Connecting to RabbitMQ", extra={"attempt": retries + 1, "max_attempts": max_retries})
            connection = pika.BlockingConnection(params)
            logger.info("Connected to RabbitMQ successfully")
            break
        except Exception as e:
            retries += 1
            wait = 2 * retries
            logger.warning("RabbitMQ not ready", extra={"error": str(e), "retry_in_seconds": wait})
            time.sleep(wait)

    if connection is None or connection.is_closed:
        logger.error("Could not connect to RabbitMQ after max retries. Exiting.")
        raise SystemExit(1)

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)

    logger.info("Worker started, waiting for messages", extra={"queue": QUEUE_NAME})
    channel.start_consuming()


if __name__ == "__main__":
    start_consumer()
