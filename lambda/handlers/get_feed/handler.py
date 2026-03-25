import os
import json
import base64
import time
import redis

VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "localhost")
DEFAULT_LIMIT = 20
MAX_LIMIT = 100

# Load pseudo-stream configuration
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
    Get Feed Skeleton endpoint: /xrpc/app.bsky.feed.getFeedSkeleton
    Returns paginated list of post URIs from Valkey.

    Query parameters:
    - feed: "raw" or "dense" (required)
    - limit: 1-100 (default: 20)
    - cursor: pagination token
    """
    try:
        # Parse query params
        params = event.get("queryStringParameters") or {}

        # Parse body for POST requests
        body = {}
        if event.get("body"):
            try:
                body = json.loads(event["body"])
            except Exception:
                body = {}

        # Get feed type (raw, dense, or stablehashtag)
        # Support both query parameter and raw query string
        feed_type = body.get("feed") or params.get("feed") or "raw"

        # Extract feed type from AT URI if needed (e.g., at://did:plc:.../app.bsky.feed.generator/japanese-raw-feed)
        if feed_type.startswith("at://"):
            # Parse rkey from AT URI
            try:
                rkey = feed_type.split("/")[-1]
                if rkey == "japanese-raw-feed":
                    feed_type = "raw"
                elif rkey == "japanese-dense-feed":
                    feed_type = "dense"
                elif rkey == "japanese-stablehashtag-feed":
                    feed_type = "stablehashtag"
            except Exception:
                pass

        # Fallback: check raw query string if params parsing failed
        if feed_type == "raw":
            raw_query = event.get("rawQueryString", "")
            if "feed=dense" in raw_query:
                feed_type = "dense"
            elif "feed=stablehashtag" in raw_query:
                feed_type = "stablehashtag"
            elif "feed=raw" in raw_query:
                feed_type = "raw"

        if feed_type not in ["raw", "dense", "stablehashtag"]:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": f"Invalid feed type '{feed_type}'. Must be 'raw', 'dense', or 'stablehashtag'."})
            }

        # Select ZSET key
        feed_key = f"feed:{feed_type}:jp:v1"

        # Debug: Check ZCARD
        try:
            zcard = r.zcard(feed_key)
            print(f"[DEBUG] ZCARD {feed_key}: {zcard}")
        except Exception as e:
            print(f"[DEBUG] ZCARD error: {e}")

        # Get limit
        limit = int(body.get("limit") or params.get("limit") or DEFAULT_LIMIT)
        if limit > MAX_LIMIT:
            limit = MAX_LIMIT
        if limit < 1:
            limit = 1

        # Get cursor
        cursor = body.get("cursor") or params.get("cursor")

        # Parse cursor
        max_score = float('inf')
        offset = 0
        if cursor:
            try:
                cursor_decoded = base64.b64decode(cursor).decode()
                max_score_str, offset_str = cursor_decoded.split(":")
                max_score = float(max_score_str)
                offset = int(offset_str)
            except Exception as e:
                # Cursor parse error - log for debugging if needed
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"Invalid cursor format: {str(e)}"})
                }

        # Pseudo-stream: only return posts from current time or earlier
        # Store Lambda distributes posts across 20-minute window starting from store time
        # Get Feed returns posts where visible_ts <= current_time for gradual appearance
        current_time = time.time()

        # Never return posts newer than current time
        max_score = min(max_score, current_time)

        raw = r.zrevrangebyscore(
            feed_key,
            max_score,
            "-inf",
            start=offset,
            num=limit + 1,
            withscores=True,
        )

        print(f"[DEBUG] zrevrangebyscore result count: {len(raw)}, max_score: {max_score}, offset: {offset}, limit: {limit}")

        # Build feed items
        # Always return requested limit regardless of batch state
        items = []
        last_member = None
        last_score = None

        for idx, (member_json, score) in enumerate(raw[:limit]):
            try:
                # Member is JSON with uri, ts, visible_ts, density_score
                member = json.loads(member_json)
                uri = member.get("uri")
                if uri:
                    items.append({"post": uri})
                    last_member = member_json
                    last_score = score
            except json.JSONDecodeError:
                # Fallback: treat as plain URI string (backward compatibility)
                items.append({"post": member_json})
                last_member = member_json
                last_score = score

        # Build next cursor if more items exist
        next_cursor = None
        if len(raw) > limit and last_score is not None:
            # Cursor format: score:offset (base64 encoded for Bluesky compatibility)
            cursor_str = f"{last_score}:{offset + limit}"
            next_cursor = base64.b64encode(cursor_str.encode()).decode()

        # Build response - cursor is optional but should be present if there are more items
        response = {
            "feed": items,
        }
        if next_cursor:
            response["cursor"] = next_cursor

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "charset": "utf-8",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps(response, ensure_ascii=False)
        }

    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[ERROR] {error_msg}")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"error": str(e)})
        }
