import os
import json
import redis
import time
import boto3

# Load configuration
def load_config():
    """Load configuration from config.json"""
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

_config = None

def get_config():
    """Get cached config"""
    global _config
    if _config is None:
        _config = load_config()
    return _config

def get_density_threshold():
    """Get density threshold from config.json"""
    config = get_config()
    return float(config["scoring"]["density_threshold"]["threshold"])

def get_batch_spread_seconds():
    """Get batch spread seconds from config.json"""
    config = get_config()
    return int(config["scheduling"]["batch_spread_seconds"])

VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "localhost")
MAX_ITEMS_RAW = 5000
MAX_ITEMS_DENSE = 2000

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
    Saves posts to Valkey ZSETs and invokes AggregationLambda.

    Expected event:
    {
        "items": [...],
        "batch_stats": {...}
    }
    """
    try:
        items = event.get("items", [])
        batch_stats = event.get("batch_stats", {})
        print(f"[DEBUG] Received items count: {len(items)}")

        if items and len(items) > 0:
            first_item = items[0]
            print(f"[DEBUG] First item: uri={first_item.get('uri')}, ts={first_item.get('ts')}, density={first_item.get('density_score')}")

        if not items:
            print("[DEBUG] No items received")
            return {"stored_raw": 0, "stored_dense": 0, "note": "no items"}

        # Test Valkey connection
        r.ping()

        now = int(time.time())
        raw_stored = 0
        dense_stored = 0

        # Calculate time distribution for batch
        batch_spread_seconds = get_batch_spread_seconds()
        MAX_ITEMS_TIME_WINDOW = batch_spread_seconds
        items_count = len(items)

        for idx, item in enumerate(items):
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

            # Calculate visible_ts: distribute posts across the batch_spread_seconds window from now
            # This ensures the latest batch is gradually displayed starting from store time
            # First post: visible_ts = now
            # Last post: visible_ts = now + batch_spread_seconds
            if items_count > 1:
                offset = (idx / (items_count - 1)) * batch_spread_seconds
            else:
                offset = 0
            visible_ts = now + offset

            # Create member as JSON with all metadata
            member = json.dumps({
                "uri": uri,
                "ts": ts,
                "visible_ts": visible_ts,
                "density_score": density_score,
                "hashtags": item.get("hashtags", [])
            }, ensure_ascii=False)

            # Always store in raw feed
            try:
                result_raw = r.zadd("feed:raw:jp:v1", {member: visible_ts})
                raw_stored += 1
                if result_raw == 0:
                    print(f"[WARN] Raw zadd returned 0 (duplicate?): {uri}")
            except Exception as e:
                print(f"[ERROR] Raw zadd failed for {uri}: {e}")

            # Store in dense feed if score >= threshold
            if density_score >= get_density_threshold():
                try:
                    result_dense = r.zadd("feed:dense:jp:v1", {member: visible_ts})
                    dense_stored += 1
                    if result_dense == 0:
                        print(f"[WARN] Dense zadd returned 0 (duplicate?): {uri}")
                except Exception as e:
                    print(f"[ERROR] Dense zadd failed for {uri}: {e}")

        # Trim both feeds to their respective limits (keep latest)
        r.zremrangebyrank("feed:raw:jp:v1", 0, -MAX_ITEMS_RAW - 1)
        r.zremrangebyrank("feed:dense:jp:v1", 0, -MAX_ITEMS_DENSE - 1)

        # Log final storage stats
        raw_zcard = r.zcard("feed:raw:jp:v1")
        dense_zcard = r.zcard("feed:dense:jp:v1")
        print(f"[STORE] Stored - Raw: {raw_stored}, Dense: {dense_stored}")
        print(f"[STORE] Final - Raw ZCARD: {raw_zcard}, Dense ZCARD: {dense_zcard}")

        # Invoke AggregationLambda asynchronously
        aggregation_function_name = os.environ.get("AGGREGATION_FUNCTION_NAME", "")
        if aggregation_function_name and batch_stats:
            try:
                lambda_client = boto3.client("lambda")
                aggregation_payload = {
                    "batch_stats": batch_stats
                }
                response = lambda_client.invoke(
                    FunctionName=aggregation_function_name,
                    InvocationType="Event",  # Asynchronous
                    Payload=json.dumps(aggregation_payload),
                )
                print(f"[STORE] AggregationLambda invoked: {response['StatusCode']}")
            except Exception as e:
                print(f"[STORE] Failed to invoke AggregationLambda: {str(e)}")

        return {
            "stored_raw": raw_stored,
            "stored_dense": dense_stored,
        }

    except Exception as e:
        # Log error for debugging
        print(f"ERROR: Store Lambda failed - {str(e)}")
        return {
            "error": str(e),
            "stored_raw": 0,
            "stored_dense": 0,
        }
