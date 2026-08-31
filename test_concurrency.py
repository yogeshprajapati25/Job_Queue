"""
Test script to demonstrate concurrency handling and rate limiting.

Usage:
    python test_concurrency.py

This simulates 20 concurrent users submitting jobs simultaneously.
"""
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "http://localhost:8000"  # Change to your deployed URL
API_KEY = "e5f2055707f2315d23b4f3162dd8226ea44355191faa690cdd8f55737f4164c4"

def submit_job(user_id: int):
    """Submit a single job and return result."""
    start_time = time.time()
    try:
        response = requests.post(
            f"{API_URL}/jobs",
            json={
                "job_type": "send_email",
                "payload": {"to": f"user{user_id}@example.com"}
            },
            headers={"X-API-Key": API_KEY},
            timeout=10
        )
        elapsed = time.time() - start_time
        
        if response.status_code == 202:
            return {
                "user_id": user_id,
                "status": "✅ SUCCESS",
                "job_id": response.json()["id"][:8],
                "elapsed": f"{elapsed:.2f}s"
            }
        elif response.status_code == 429:
            return {
                "user_id": user_id,
                "status": "⚠️  RATE LIMITED",
                "message": response.json().get("detail", {}).get("message", "Rate limit exceeded"),
                "elapsed": f"{elapsed:.2f}s"
            }
        else:
            return {
                "user_id": user_id,
                "status": f"❌ ERROR {response.status_code}",
                "message": response.text[:50],
                "elapsed": f"{elapsed:.2f}s"
            }
    except Exception as e:
        elapsed = time.time() - start_time
        return {
            "user_id": user_id,
            "status": "❌ EXCEPTION",
            "message": str(e)[:50],
            "elapsed": f"{elapsed:.2f}s"
        }


def test_concurrent_requests(num_users: int = 20):
    """Simulate multiple users submitting jobs concurrently."""
    print(f"\n{'='*70}")
    print(f"🚀 CONCURRENCY TEST: {num_users} simultaneous requests")
    print(f"{'='*70}\n")
    
    print(f"⏱️  Starting test at {time.strftime('%H:%M:%S')}")
    start_time = time.time()
    
    # Submit all requests concurrently
    with ThreadPoolExecutor(max_workers=num_users) as executor:
        futures = [executor.submit(submit_job, i+1) for i in range(num_users)]
        results = [future.result() for future in as_completed(futures)]
    
    total_time = time.time() - start_time
    
    # Sort by user_id for display
    results.sort(key=lambda x: x["user_id"])
    
    # Print results
    print(f"\n📊 RESULTS:\n")
    success_count = 0
    rate_limited_count = 0
    error_count = 0
    
    for result in results:
        print(f"  User {result['user_id']:2d}: {result['status']:<20} {result.get('job_id', '')} ({result['elapsed']})")
        if result['status'] == "✅ SUCCESS":
            success_count += 1
        elif result['status'] == "⚠️  RATE LIMITED":
            rate_limited_count += 1
        else:
            error_count += 1
    
    # Summary
    print(f"\n{'='*70}")
    print(f"📈 SUMMARY:")
    print(f"   Total Requests:    {num_users}")
    print(f"   ✅ Successful:      {success_count}")
    print(f"   ⚠️  Rate Limited:    {rate_limited_count}")
    print(f"   ❌ Errors:          {error_count}")
    print(f"   ⏱️  Total Time:      {total_time:.2f}s")
    print(f"   📊 Throughput:      {num_users/total_time:.1f} req/s")
    print(f"{'='*70}\n")


def test_rate_limit_recovery():
    """Test that rate limit resets after waiting."""
    print(f"\n{'='*70}")
    print(f"⏳ RATE LIMIT RECOVERY TEST")
    print(f"{'='*70}\n")
    
    # Submit 10 requests (should succeed)
    print("Step 1: Submitting 10 requests (should succeed)...")
    for i in range(10):
        result = submit_job(i+1)
        print(f"  Request {i+1}: {result['status']}")
    
    # Try 11th request (should be rate limited)
    print("\nStep 2: Submitting 11th request (should be rate limited)...")
    result = submit_job(11)
    print(f"  Request 11: {result['status']}")
    
    if "RATE LIMITED" in result['status']:
        print("\n✅ Rate limiting working correctly!")
        print("⏳ Waiting 60 seconds for rate limit to reset...")
        time.sleep(60)
        
        print("\nStep 3: Submitting request after 60s wait (should succeed)...")
        result = submit_job(12)
        print(f"  Request 12: {result['status']}")
        
        if result['status'] == "✅ SUCCESS":
            print("\n✅ Rate limit recovery working correctly!")
    
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    import sys
    
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                  CONCURRENCY & RATE LIMIT TESTER                 ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    print("What would you like to test?\n")
    print("1. Concurrent requests (20 simultaneous users)")
    print("2. Rate limit recovery (submit >10 requests)")
    print("3. Both tests")
    print("4. Custom concurrent request count\n")
    
    choice = input("Enter choice (1-4): ").strip()
    
    if choice == "1":
        test_concurrent_requests(20)
    elif choice == "2":
        test_rate_limit_recovery()
    elif choice == "3":
        test_concurrent_requests(20)
        test_rate_limit_recovery()
    elif choice == "4":
        num = int(input("Enter number of concurrent requests: "))
        test_concurrent_requests(num)
    else:
        print("Invalid choice. Running default test...")
        test_concurrent_requests(20)
