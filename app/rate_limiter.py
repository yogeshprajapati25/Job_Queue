"""
Distributed rate limiter using Redis.

Limits job submissions per API key to prevent abuse and ensure fair resource allocation.
Uses sliding window algorithm for accurate rate limiting across distributed API instances.
"""
import os
import time
from typing import Tuple
from fastapi import HTTPException, status

# Try to import redis, fall back to in-memory rate limiting if not available
try:
    import redis
    REDIS_AVAILABLE = True
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
except ImportError:
    REDIS_AVAILABLE = False
    # In-memory fallback for local development
    _memory_store = {}


# Rate limit configuration
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "10"))  # Max requests
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))      # Time window in seconds


def check_rate_limit(api_key: str) -> Tuple[bool, int]:
    """
    Check if the API key has exceeded the rate limit.
    
    Uses sliding window algorithm:
    - Tracks timestamps of requests in a sorted set (Redis) or list (memory)
    - Removes timestamps older than the window
    - Counts remaining timestamps
    - Allows request if count < limit
    
    Args:
        api_key: The API key to check
        
    Returns:
        Tuple of (allowed: bool, remaining: int)
        - allowed: True if request is allowed, False if rate limited
        - remaining: Number of requests remaining in current window
        
    Example:
        allowed, remaining = check_rate_limit("key123")
        if not allowed:
            raise HTTPException(429, "Rate limit exceeded")
    """
    current_time = time.time()
    window_start = current_time - RATE_LIMIT_WINDOW
    key = f"rate_limit:{api_key}"
    
    if REDIS_AVAILABLE:
        try:
            # Use Redis sorted set for distributed rate limiting
            pipe = redis_client.pipeline()
            
            # Remove old timestamps outside the window
            pipe.zremrangebyscore(key, 0, window_start)
            
            # Count requests in current window
            pipe.zcard(key)
            
            # Add current timestamp
            pipe.zadd(key, {str(current_time): current_time})
            
            # Set expiry to clean up old keys
            pipe.expire(key, RATE_LIMIT_WINDOW + 10)
            
            results = pipe.execute()
            request_count = results[1]  # Count before adding current request
            
            allowed = request_count < RATE_LIMIT_REQUESTS
            remaining = max(0, RATE_LIMIT_REQUESTS - request_count - (1 if allowed else 0))
            
            # If not allowed, remove the timestamp we just added
            if not allowed:
                redis_client.zrem(key, str(current_time))
            
            return allowed, remaining
            
        except redis.RedisError:
            # If Redis fails, fall back to allowing the request (fail open)
            return True, RATE_LIMIT_REQUESTS - 1
    else:
        # In-memory fallback (not suitable for production with multiple API instances)
        if key not in _memory_store:
            _memory_store[key] = []
        
        # Remove old timestamps
        _memory_store[key] = [ts for ts in _memory_store[key] if ts > window_start]
        
        request_count = len(_memory_store[key])
        allowed = request_count < RATE_LIMIT_REQUESTS
        
        if allowed:
            _memory_store[key].append(current_time)
        
        remaining = max(0, RATE_LIMIT_REQUESTS - request_count - (1 if allowed else 0))
        return allowed, remaining


def rate_limit_dependency(api_key: str) -> None:
    """
    FastAPI dependency that enforces rate limiting.
    
    Raises HTTPException 429 if rate limit is exceeded.
    Adds rate limit headers to response.
    
    Usage:
        @app.post("/jobs", dependencies=[Depends(rate_limit_dependency)])
        def create_job(...):
            ...
    """
    allowed, remaining = check_rate_limit(api_key)
    
    if not allowed:
        retry_after = RATE_LIMIT_WINDOW
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "Rate limit exceeded",
                "message": f"Maximum {RATE_LIMIT_REQUESTS} requests per {RATE_LIMIT_WINDOW} seconds allowed",
                "retry_after": retry_after,
                "limit": RATE_LIMIT_REQUESTS,
                "window": RATE_LIMIT_WINDOW
            },
            headers={
                "X-RateLimit-Limit": str(RATE_LIMIT_REQUESTS),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(int(time.time() + retry_after)),
                "Retry-After": str(retry_after)
            }
        )
    
    # Request allowed - could add headers here if needed
    # (Note: FastAPI dependencies can't modify response headers directly,
    #  but middleware could add these headers based on the rate limit check)
