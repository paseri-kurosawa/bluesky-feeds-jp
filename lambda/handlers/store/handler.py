import os
import json
import redis
import time

VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "localhost")
DENSITY_THRESHOLD = float(os.environ.get("DENSITY_THRESHOLD", "2.0"))
MAX_ITEMS = 5000

r = redis.Redis(
    host=VALKEY_ENDPOINT,
    port=6379,
    ssl=True,
    ssl_cert_reqs="required",
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)

def lambda_handler(event, context):
    """
    Store Lambda: Invoked asynchronously by Ingest Lambda.
    Saves posts to Valkey ZSETs.

    Expected event:
    {
        "items": [
            {"uri": "at://...", "ts": 1234567890, "density_score": 2.5},
            ...
        ]
    }
    """
    try:
        print(f"Store Lambda event: {json.dumps(event)}")

        items = event.get("items", [])
        print(f"Items count: {len(items)}")

        if not items:
            return {"stored_raw": 0, "stored_dense": 0, "note": "no items"}

        # Test Valkey connection
        r.ping()

        now = int(time.time())
        raw_stored = 0
        dense_stored = 0

        for item in items:
            uri = item.get("uri")
            ts = item.get("ts")
            density_score = item.get("density_score", 0)

            if not uri or ts is None:
                continue

            # Validate timestamp
            if ts > now + 300:
                ts = now
            if ts < 0:
                continue

            # Always store in raw feed
            r.zadd("feed:raw:jp:v1", {uri: ts})
            raw_stored += 1

            # Store in dense feed if score >= threshold
            if density_score >= DENSITY_THRESHOLD:
                r.zadd("feed:dense:jp:v1", {uri: ts})
                dense_stored += 1

        # Trim both feeds to MAX_ITEMS (keep latest)
        r.zremrangebyrank("feed:raw:jp:v1", 0, -MAX_ITEMS - 1)
        r.zremrangebyrank("feed:dense:jp:v1", 0, -MAX_ITEMS - 1)

        print(f"Stored - Raw: {raw_stored}, Dense: {dense_stored}")
        print(f"Valkey - Raw ZCARD: {r.zcard('feed:raw:jp:v1')}, Dense ZCARD: {r.zcard('feed:dense:jp:v1')}")

        return {
            "stored_raw": raw_stored,
            "stored_dense": dense_stored,
        }

    except Exception as e:
        print(f"Store Lambda error: {str(e)}")
        return {
            "error": str(e),
            "stored_raw": 0,
            "stored_dense": 0,
        }
