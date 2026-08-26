import json
import os
import pika
import threading

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

# One connection + channel shared across all requests in a process.
# A threading.Lock guards against concurrent access (FastAPI uses a thread pool).
_connection: pika.BlockingConnection | None = None
_channel: pika.adapters.blocking_connection.BlockingChannel | None = None
_lock = threading.Lock()

QUEUE_NAME = "job_queue"


def _get_channel():
    """Return a live channel, (re)connecting if the previous one dropped."""
    global _connection, _channel

    with _lock:
        # Re-establish if connection is missing or has been closed
        if _connection is None or _connection.is_closed:
            params = pika.URLParameters(RABBITMQ_URL)
            _connection = pika.BlockingConnection(params)
            _channel = None  # force channel recreation too

        if _channel is None or _channel.is_closed:
            _channel = _connection.channel()
            _channel.queue_declare(queue=QUEUE_NAME, durable=True)

        return _channel


def publish_job(job_id: str, job_type: str) -> None:
    """
    Publish a job message to RabbitMQ.
    Reuses a persistent connection instead of opening one per request.
    Falls back to a fresh connection on any channel-level error.
    """
    message = json.dumps({"job_id": job_id, "job_type": job_type})

    try:
        channel = _get_channel()
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent  # survives broker restart
            ),
        )
    except Exception:
        # Connection or channel died mid-flight — reset and retry once
        global _connection, _channel
        with _lock:
            _connection = None
            _channel = None

        channel = _get_channel()
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ),
        )
