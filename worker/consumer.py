import json
import os
import random
import time
import pika
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Job, JobStatus

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
QUEUE_NAME = "job_queue"


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
    """Simulate generating a PDF/CSV report."""
    time.sleep(3)
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


# Maps job_type strings to their handler functions
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

    try:
        data = json.loads(body)
        job_id = data.get("job_id")

        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            print(f"❌ Job {job_id} not found — ACKing to discard stale message.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"⚙️  Processing job {job_id} | type={job.job_type} | attempt={job.retry_count + 1}/{job.max_retries + 1}")

        # Mark as processing
        job.status = JobStatus.PROCESSING
        db.commit()

        # Run the handler
        result = process_job(job.job_type, job.payload)

        # Success — persist result and mark completed
        job.status = JobStatus.COMPLETED
        job.result = result
        job.error_message = None
        db.commit()

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"✅ Job {job_id} completed. Result: {result}")

    except Exception as e:
        db.rollback()
        error_str = str(e)
        print(f"⚠️  Error on job {job_id}: {error_str}")

        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            job.retry_count += 1
            job.error_message = error_str

            if job.retry_count > job.max_retries:
                # Exceeded retries — dead-letter in DB and ACK to remove from queue
                job.status = JobStatus.DEAD_LETTER
                db.commit()
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f"💀 Job {job_id} moved to DEAD_LETTER after {job.max_retries} retries.")
            else:
                job.status = JobStatus.FAILED
                db.commit()

                # Exponential backoff: wait before NACKing so the broker
                # doesn't immediately re-deliver to another worker.
                # requeue=False drops it; we re-publish manually after the delay
                # so the message goes to the back of the queue, not the front.
                backoff = 2 ** job.retry_count
                print(f"🔄 Retrying job {job_id} in {backoff}s (attempt {job.retry_count}/{job.max_retries})...")
                time.sleep(backoff)

                # Re-publish to the back of the queue instead of NACKing to front
                ch.basic_ack(delivery_tag=method.delivery_tag)
                ch.basic_publish(
                    exchange="",
                    routing_key=QUEUE_NAME,
                    body=body,  # same message body
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent
                    ),
                )
        else:
            # Can't find the job at all — discard the message
            ch.basic_ack(delivery_tag=method.delivery_tag)

    finally:
        db.close()


# ---------------------------------------------------------------------------
# Worker entry point
# ---------------------------------------------------------------------------

def start_consumer():
    # Retry loop — RabbitMQ may not be ready immediately even after health check passes
    params = pika.URLParameters(RABBITMQ_URL)
    connection = None
    retries = 0
    max_retries = 10

    while retries < max_retries:
        try:
            print(f"🔌 Connecting to RabbitMQ (attempt {retries + 1}/{max_retries})...")
            connection = pika.BlockingConnection(params)
            print("✅ Connected to RabbitMQ.")
            break
        except Exception as e:
            retries += 1
            wait = 2 * retries  # 2s, 4s, 6s ...
            print(f"⚠️  RabbitMQ not ready: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    if connection is None or connection.is_closed:
        print("❌ Could not connect to RabbitMQ after max retries. Exiting.")
        raise SystemExit(1)

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # prefetch_count=1: don't give a worker a second message until it ACKs the first.
    # This is what enables fair dispatch across multiple worker replicas.
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)

    print("🚀 Worker started. Waiting for messages. Press CTRL+C to exit.")
    channel.start_consuming()


if __name__ == "__main__":
    start_consumer()
