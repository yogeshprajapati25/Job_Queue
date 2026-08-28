import json
import os
import pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
QUEUE_NAME = "job_queue"


def publish_job(job_id: str, job_type: str) -> None:
    """
    Publish a job message to RabbitMQ.
    Opens a fresh connection per publish — required for CloudAMQP free tier
    which limits concurrent connections to 1 per service.
    Connection is always closed after publishing to free the slot.
    """
    message = json.dumps({"job_id": job_id, "job_type": job_type})

    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ),
        )
    finally:
        connection.close()
