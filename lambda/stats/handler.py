import json
import os
from datetime import datetime, timedelta
import boto3
import redis

s3_client = boto3.client("s3")
cloudwatch_client = boto3.client("cloudwatch")

# Valkey connection
VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "localhost")
try:
    valkey_client = redis.Redis(
        host=VALKEY_ENDPOINT,
        port=6379,
        ssl=True,
        ssl_cert_reqs="required",
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=10,
    )
    valkey_client.ping()
    print("[VALKEY] Connected successfully")
except Exception as e:
    print(f"[VALKEY] Connection failed: {str(e)}")
    valkey_client = None

def get_jst_now():
    """Get current time in JST (UTC+9)"""
    return datetime.utcnow() + timedelta(hours=9)


def save_batch_stats(bucket, timestamp, batch_stats):
    """Save batch statistics to S3"""
    s3_key = f"stats/batch/stats_{timestamp}.json"
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(batch_stats, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved batch stats to {s3_key}")
        return s3_key
    except Exception as e:
        print(f"[S3] Error saving batch stats: {str(e)}")
        return None


def lambda_handler(event, context):
    """
    StatsLambda: Process payloads from Valkey stats:queue and save to S3

    Workflow:
    1. Scan Valkey stats:queue:* entries
    2. Find entries with processed: False
    3. Mark as processed: True
    4. Extract batch_stats and top_hashtags
    5. Save to S3 (stats/batch/stats_{timestamp}.json)
    6. Delete queue entry
    """

    try:
        bucket = os.environ.get("STATISTICS_BUCKET", "")
        if not bucket:
            raise ValueError("STATISTICS_BUCKET environment variable not set")

        if not valkey_client:
            print("[STATS] Valkey client not available")
            return {"status": "error", "reason": "valkey_unavailable"}

        # === Step 1: Scan Valkey for unprocessed payloads ===
        print("[STATS] Scanning Valkey stats:queue for unprocessed entries")
        queue_keys = valkey_client.keys("stats:queue:*")
        print(f"[STATS] Found {len(queue_keys)} queue entries")

        processed_count = 0
        for queue_key in queue_keys:
            try:
                # Check if already processed
                processed_flag = valkey_client.hget(queue_key, "processed")
                if processed_flag and processed_flag.lower() == 'true':
                    print(f"[STATS] Skipping {queue_key} (already processed)")
                    continue

                # Get payload
                payload_json = valkey_client.hget(queue_key, "payload")
                if not payload_json:
                    print(f"[STATS] No payload in {queue_key}, deleting")
                    valkey_client.delete(queue_key)
                    continue

                # Mark as processed (BEFORE processing to prevent race conditions)
                valkey_client.hset(queue_key, "processed", True)
                print(f"[STATS] Marked {queue_key} as processed")

                # Parse payload
                try:
                    payload = json.loads(payload_json)
                    batch_stats = payload.get("batch_stats", {})
                    top_hashtags = payload.get("top_hashtags", [])
                except json.JSONDecodeError as e:
                    print(f"[STATS] Failed to parse payload JSON: {str(e)}")
                    valkey_client.delete(queue_key)
                    continue

                if not batch_stats:
                    print(f"[STATS] No batch_stats in {queue_key}, deleting")
                    valkey_client.delete(queue_key)
                    continue

                # Extract timestamp
                timestamp = batch_stats.get("timestamp", get_jst_now().strftime("%Y%m%d_%H%M%S"))

                # === Step 2: Save batch stats to S3 ===
                saved_key = save_batch_stats(bucket, timestamp, batch_stats)
                if saved_key:
                    processed_count += 1
                    print(f"[STATS] Successfully processed {queue_key}")

                # === Step 3: Delete processed entry from Valkey ===
                valkey_client.delete(queue_key)
                print(f"[STATS] Deleted {queue_key} from queue")

            except Exception as e:
                print(f"[STATS] Error processing {queue_key}: {str(e)}")
                import traceback
                traceback.print_exc()
                # Continue to next entry even if one fails

        print(f"[STATS] Processed {processed_count} entries")
        return {
            "status": "success",
            "processed_count": processed_count
        }

    except Exception as e:
        print(f"[STATS] Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "error": str(e)
        }
