import json
import os
import pika
import threading
from queue import Queue, Empty
from contextlib import contextmanager

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
QUEUE_NAME = "job_queue"

# Connection pool: Maintains 3 reusable RabbitMQ connections
# This reduces connection overhead and stays well under CloudAMQP's 20 connection limit
POOL_SIZE = 3
_connection_pool = Queue(maxsize=POOL_SIZE)
_pool_lock = threading.Lock()
_pool_initialized = False


def _initialize_pool():
    """Initialize the connection pool with POOL_SIZE connections."""
    global _pool_initialized
    with _pool_lock:
        if _pool_initialized:
            return
        
        params = pika.URLParameters(RABBITMQ_URL)
        for _ in range(POOL_SIZE):
            try:
                connection = pika.BlockingConnection(params)
                _connection_pool.put(connection)
            except Exception as e:
                # If pool initialization fails, continue with fewer connections
                print(f"Warning: Failed to initialize RabbitMQ connection: {e}")
                break
        
        _pool_initialized = True


@contextmanager
def _get_connection():
    """
    Context manager to borrow a connection from the pool.
    If pool is empty, waits up to 5 seconds for a connection to become available.
    """
    if not _pool_initialized:
        _initialize_pool()
    
    connection = None
    try:
        # Wait up to 5 seconds for an available connection
        connection = _connection_pool.get(timeout=5)
        
        # Check if connection is still open, reconnect if needed
        if connection.is_closed:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
        
        yield connection
        
    except Empty:
        # All connections busy - fall back to creating a temporary one
        params = pika.URLParameters(RABBITMQ_URL)
        temp_connection = pika.BlockingConnection(params)
        try:
            yield temp_connection
        finally:
            temp_connection.close()
    finally:
        # Return connection to pool if it was borrowed
        if connection is not None:
            try:
                _connection_pool.put_nowait(connection)
            except:
                # Pool is full (shouldn't happen, but handle gracefully)
                pass


def publish_job(job_id: str, job_type: str) -> None:
    """
    Publish a job message to RabbitMQ using a pooled connection.
    
    Connection pooling benefits:
    - Reuses 3 persistent connections instead of creating new ones per request
    - Reduces latency (no connection handshake overhead)
    - Stays well under CloudAMQP's 20 connection limit
    - Handles high concurrency gracefully with connection queuing
    """
    message = json.dumps({"job_id": job_id, "job_type": job_type})

    with _get_connection() as connection:
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
        channel.close()
