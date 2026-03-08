import os
import json
import base64
import redis

VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "localhost")
DEFAULT_LIMIT = 20
MAX_LIMIT = 100

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
        # Debug: log incoming event
        print(f"Event: {json.dumps(event)}")

        # Parse query params
        params = event.get("queryStringParameters") or {}

        # Parse body for POST requests
        body = {}
        if event.get("body"):
            try:
                body = json.loads(event["body"])
            except Exception:
                body = {}

        # Get feed type (raw or dense)
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
            except Exception:
                pass

        # Fallback: check raw query string if params parsing failed
        if feed_type == "raw":
            raw_query = event.get("rawQueryString", "")
            if "feed=dense" in raw_query:
                feed_type = "dense"
            elif "feed=raw" in raw_query:
                feed_type = "raw"

        if feed_type not in ["raw", "dense"]:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": f"Invalid feed type '{feed_type}'. Must be 'raw' or 'dense'."})
            }

        # Select ZSET key
        feed_key = f"feed:{feed_type}:jp:v1"

        # Get limit
        limit = int(body.get("limit") or params.get("limit") or DEFAULT_LIMIT)
        if limit > MAX_LIMIT:
            limit = MAX_LIMIT
        if limit < 1:
            limit = 1

        # Get cursor
        cursor = body.get("cursor") or params.get("cursor")

        # Parse cursor
        max_score = "+inf"
        offset = 0
        if cursor:
            try:
                cursor_decoded = base64.b64decode(cursor).decode()
                max_score_str, offset_str = cursor_decoded.split(":")
                max_score = float(max_score_str)
                offset = int(offset_str)
            except Exception as e:
                print(f"Cursor parse error: {e}, raw cursor: {cursor}")
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"Invalid cursor format: {str(e)}"})
                }

        # Fetch from ZSET (sorted by score descending = latest first)
        raw = r.zrevrangebyscore(
            feed_key,
            max_score,
            "-inf",
            start=offset,
            num=limit + 1,
            withscores=True,
        )

        # Build feed items
        items = []
        for uri, score in raw[:limit]:
            items.append({"post": uri})

        # Build next cursor if more items exist
        next_cursor = None
        if len(raw) > limit:
            last_uri, last_score = raw[limit - 1]
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
                "charset": "utf-8"
            },
            "body": json.dumps(response, ensure_ascii=False)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
