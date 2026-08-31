# 🚀 Concurrency & Rate Limiting Architecture

This document explains how the job queue system handles concurrent requests and prevents abuse through rate limiting.

---

## 📊 Problem: What Happens with 100 Concurrent Users?

### **Before Optimization:**

```
100 users submit jobs simultaneously:
├─ Database: 5 connection pool → 95 requests wait in queue
├─ RabbitMQ: 100 new connections → Exceeds 20 connection limit → 80 FAIL ❌
└─ No rate limiting → Single user can submit 1000 jobs/second
```

### **After Optimization:**

```
100 users submit jobs simultaneously:
├─ Rate Limiter: 10 requests/minute per API key → 90 get "429 Rate Limited" ✅
├─ Database: 5 connection pool → First 5 process, others queue gracefully ✅
├─ RabbitMQ: 3 pooled connections → All share 3 connections → No failures ✅
└─ Result: System stays stable, no crashes, fair resource allocation ✅
```

---

## 🔧 Implementation Details

### **1. RabbitMQ Connection Pooling**

**File:** `app/producer.py`

**How it works:**
```python
# OLD (creates new connection per request):
def publish_job():
    connection = pika.BlockingConnection(params)  # ❌ Opens new connection
    channel = connection.channel()
    channel.basic_publish(...)
    connection.close()  # ❌ Closes connection

# NEW (reuses 3 persistent connections):
connection_pool = Queue(maxsize=3)  # ✅ Pool of 3 connections

@contextmanager
def _get_connection():
    connection = connection_pool.get()  # ✅ Borrow from pool
    yield connection
    connection_pool.put(connection)      # ✅ Return to pool

def publish_job():
    with _get_connection() as connection:
        channel = connection.channel()
        channel.basic_publish(...)
        # Connection automatically returned to pool
```

**Benefits:**
- ✅ Only uses 3 CloudAMQP connections (leaves 17 available for workers)
- ✅ Faster (no connection handshake overhead ~100-200ms saved)
- ✅ Handles 100+ concurrent requests gracefully (requests queue for available connection)
- ✅ Automatic reconnection if connection dies

**Concurrency Flow (20 requests):**
```
Time 0ms:  Request 1-3 grab connections [Pool: 0/3 available]
Time 5ms:  Request 4-20 waiting for connection [Pool: 0/3 available]
Time 10ms: Request 1 returns connection [Pool: 1/3 available]
Time 10ms: Request 4 grabs connection [Pool: 0/3 available]
... continues until all 20 complete
```

---

### **2. Distributed Rate Limiting**

**File:** `app/rate_limiter.py`

**Algorithm:** Sliding Window (using Redis Sorted Sets)

**How it works:**
```python
# Rate limit: 10 requests per 60 seconds per API key

# Request 1-10 (within 60 seconds):
check_rate_limit(api_key="abc123")
→ Redis: Add timestamp to sorted set "rate_limit:abc123"
→ Count timestamps in last 60 seconds: 1, 2, 3... 10
→ Result: ALLOWED ✅ (count < 10)

# Request 11 (within same 60 seconds):
check_rate_limit(api_key="abc123")
→ Redis: Count timestamps in last 60 seconds: 10
→ Result: RATE LIMITED ❌ (count >= 10)
→ HTTP 429: "Retry after 60 seconds"

# After 60 seconds:
check_rate_limit(api_key="abc123")
→ Redis: Old timestamps expired, count = 0
→ Result: ALLOWED ✅ (fresh window)
```

**Why Redis?**
- ✅ **Distributed:** Multiple API instances share same rate limit
- ✅ **Accurate:** Sliding window (not fixed window)
- ✅ **Fast:** O(log N) operations
- ✅ **Graceful fallback:** Uses in-memory store if Redis unavailable

**Configuration:**
```bash
# Environment variables
RATE_LIMIT_REQUESTS=10   # Max requests
RATE_LIMIT_WINDOW=60     # Time window (seconds)
REDIS_URL=redis://...    # Optional (falls back to in-memory)
```

**Response when rate limited:**
```json
HTTP 429 Too Many Requests
{
  "detail": {
    "error": "Rate limit exceeded",
    "message": "Maximum 10 requests per 60 seconds allowed",
    "retry_after": 60,
    "limit": 10,
    "window": 60
  }
}
Headers:
  Retry-After: 60
```

---

### **3. Database Connection Pool**

**File:** `app/database.py`

**How it works:**
```python
# SQLAlchemy automatically creates connection pool
engine = create_engine(DATABASE_URL)
# Default: pool_size=5, max_overflow=10 (up to 15 connections)

# Request lifecycle:
1. Request arrives → get_db() called
2. SessionLocal() borrows connection from pool
3. Query executes using borrowed connection
4. db.close() returns connection to pool
5. Next request reuses same connection
```

**Benefits:**
- ✅ Reuses 5-15 connections for all requests
- ✅ No "too many connections" errors
- ✅ Efficient resource usage

**Concurrency Flow (20 requests):**
```
Time 0ms:  Request 1-5 grab DB connections [Pool: 0/5 available, 0/10 overflow]
Time 5ms:  Request 6-10 create overflow connections [Pool: 0/5, 5/10 overflow]
Time 10ms: Request 11-15 create more overflow [Pool: 0/5, 10/10 overflow]
Time 15ms: Request 16-20 WAIT for connection [Pool: FULL]
Time 20ms: Request 1 finishes, returns connection [Pool: 1/5 available]
Time 20ms: Request 16 grabs available connection [Pool: 0/5 available]
... continues
```

---

## 🧪 Testing Concurrency

### **Run Test Script:**

```bash
python test_concurrency.py
```

**Test 1: 20 Concurrent Requests**
```
🚀 CONCURRENCY TEST: 20 simultaneous requests
========================================

  User  1: ✅ SUCCESS           (0.15s)
  User  2: ✅ SUCCESS           (0.16s)
  ...
  User 10: ✅ SUCCESS           (0.21s)
  User 11: ⚠️  RATE LIMITED      (0.12s)
  ...
  User 20: ⚠️  RATE LIMITED      (0.13s)

📈 SUMMARY:
   Total Requests:    20
   ✅ Successful:      10
   ⚠️  Rate Limited:    10
   ❌ Errors:          0
   ⏱️  Total Time:      0.35s
   📊 Throughput:      57.1 req/s
```

---

## 📚 Interview Talking Points

### **Q: How does your system handle 100 concurrent requests?**

**Answer:**
> "The system uses three layers of concurrency control:
> 
> 1. **Rate Limiting**: Redis-based sliding window limits each API key to 10 requests/minute, preventing abuse and ensuring fair resource allocation across users.
> 
> 2. **Connection Pooling**: RabbitMQ uses a pool of 3 persistent connections shared by all requests, staying well under CloudAMQP's 20 connection limit while eliminating connection overhead.
> 
> 3. **Database Pooling**: SQLAlchemy maintains 5-15 reusable database connections, allowing concurrent queries without exhausting database connections.
> 
> Under peak load, requests queue gracefully at each layer with proper backpressure instead of failing with connection errors."

---

### **Q: Why use Redis for rate limiting instead of in-memory storage?**

**Answer:**
> "In-memory rate limiting only works for single-instance deployments. With multiple API instances behind a load balancer, each instance would track limits independently, allowing users to bypass the limit by hitting different instances.
> 
> Redis provides distributed state, so all API instances share the same rate limit counters. I use a sliding window algorithm with Redis sorted sets for accurate per-second granularity. The implementation also has graceful degradation — if Redis is unavailable, it falls back to in-memory limiting rather than rejecting all requests."

---

### **Q: What happens if RabbitMQ is slow?**

**Answer:**
> "The connection pool uses a 5-second timeout when borrowing connections. If all 3 pooled connections are busy (waiting on slow RabbitMQ operations), request #4 waits up to 5 seconds. If still no connection available, it falls back to creating a temporary connection.
> 
> This provides backpressure — if RabbitMQ is struggling, the API naturally slows down job acceptance rather than queueing infinite jobs in memory. Clients get slower responses but not errors. For production, I'd add metrics to monitor connection pool wait times and alert if p95 latency exceeds thresholds."

---

### **Q: How would you scale this to 10,000 requests/second?**

**Optimizations:**
1. **Horizontal scaling**: Deploy 10+ API instances behind load balancer
2. **Increase rate limits**: 100 req/min per user (10,000 users → 16,666 req/min peak)
3. **Partition RabbitMQ**: Multiple queues (by job type or hash) with dedicated workers
4. **Database read replicas**: Route GET /jobs to replicas, POST /jobs to primary
5. **Async workers**: Replace sync SQLAlchemy with async (asyncpg) for non-blocking DB
6. **Redis cluster**: Shard rate limit keys across Redis cluster nodes
7. **Batch publishing**: Bundle multiple job messages into single RabbitMQ publish

---

## 🎯 Key Takeaways

| Component | Without Optimization | With Optimization |
|-----------|---------------------|-------------------|
| **RabbitMQ Connections** | 1 per request (20 max) | 3 pooled (reused) |
| **Database Connections** | 1 per request | 5-15 pooled (reused) |
| **Rate Limiting** | None (abuse possible) | 10 req/min per key |
| **100 Concurrent Requests** | 80+ failures | 10 succeed, 90 rate limited gracefully |
| **Response Under Load** | Connection errors | Controlled backpressure |

---

**Deployment Note:** Redis is optional. The rate limiter automatically falls back to in-memory storage if Redis is unavailable, making it work on Render free tier without requiring paid Redis.
