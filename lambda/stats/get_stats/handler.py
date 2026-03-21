import os
import json
import redis
import boto3
from datetime import datetime, timedelta

# Valkey connection
VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "localhost")
PUT_STATS_FUNCTION_NAME = os.environ.get("PUT_STATS_FUNCTION_NAME", "")

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


def lambda_handler(event, context):
    """
    GetStatsLambda: Scan Valkey stats:queue for unprocessed payloads

    Workflow:
    1. Scan Valkey stats:queue:* entries
    2. Find entries with processed: "False"
    3. Mark as processed: "True"
    4. Extract batch_stats and top_hashtags
    5. Invoke PutStatsLambda (synchronously)
    6. On success, delete queue entry
    7. Continue to next entry
    """

    try:
        if not valkey_client:
            print("[GET_STATS] Valkey client not available")
            return {"status": "error", "reason": "valkey_unavailable"}

        if not PUT_STATS_FUNCTION_NAME:
            print("[GET_STATS] PUT_STATS_FUNCTION_NAME not set")
            return {"status": "error", "reason": "put_stats_function_not_set"}

        # === Step 1: Scan Valkey for unprocessed payloads ===
        print("[GET_STATS] Scanning Valkey stats:queue for unprocessed entries")

        # Use SCAN instead of KEYS (Valkey may not support KEYS)
        queue_keys = []
        try:
            cursor = 0
            while True:
                cursor, keys = valkey_client.scan(cursor, match="stats:queue:*", count=100)
                queue_keys.extend(keys)
                if cursor == 0:
                    break
        except Exception as e:
            print(f"[GET_STATS] SCAN failed, trying KEYS: {str(e)}")
            try:
                queue_keys = valkey_client.keys("stats:queue:*")
            except Exception as e2:
                print(f"[GET_STATS] Both SCAN and KEYS failed: {str(e2)}")
                queue_keys = []

        print(f"[GET_STATS] Found {len(queue_keys)} queue entries")

        processed_count = 0
        error_count = 0
        lambda_client = boto3.client("lambda", region_name="ap-northeast-1")

        for queue_key in queue_keys:
            try:
                # Check if already processed
                processed_flag = valkey_client.hget(queue_key, "processed")
                if processed_flag and processed_flag.lower() == 'true':
                    print(f"[GET_STATS] Skipping {queue_key} (already processed)")
                    continue

                # Get payload
                payload_json = valkey_client.hget(queue_key, "payload")
                if not payload_json:
                    print(f"[GET_STATS] No payload in {queue_key}, deleting")
                    valkey_client.delete(queue_key)
                    continue

                # Mark as processed (BEFORE invoking PutStats to prevent race conditions)
                valkey_client.hset(queue_key, "processed", "True")
                print(f"[GET_STATS] Marked {queue_key} as processed")

                # Parse payload
                try:
                    payload = json.loads(payload_json)
                    batch_stats = payload.get("batch_stats", {})
                    top_hashtags = payload.get("top_hashtags", [])
                except json.JSONDecodeError as e:
                    print(f"[GET_STATS] Failed to parse payload JSON: {str(e)}")
                    valkey_client.delete(queue_key)
                    continue

                if not batch_stats:
                    print(f"[GET_STATS] No batch_stats in {queue_key}, deleting")
                    valkey_client.delete(queue_key)
                    continue

                # === Step 2: Invoke PutStatsLambda synchronously ===
                print(f"[GET_STATS] Invoking PutStatsLambda for {queue_key}")
                put_stats_payload = {
                    "batch_stats": batch_stats,
                    "top_hashtags": top_hashtags,
                    "queue_key": queue_key
                }

                try:
                    response = lambda_client.invoke(
                        FunctionName=PUT_STATS_FUNCTION_NAME,
                        InvocationType="RequestResponse",  # Synchronous
                        Payload=json.dumps(put_stats_payload),
                    )

                    if response['StatusCode'] == 200:
                        response_payload = json.loads(response['Payload'].read())
                        print(f"[GET_STATS] PutStatsLambda success: {response_payload}")

                        # === Step 3: Delete processed entry from Valkey ===
                        valkey_client.delete(queue_key)
                        print(f"[GET_STATS] Deleted {queue_key} from queue")
                        processed_count += 1
                    else:
                        print(f"[GET_STATS] PutStatsLambda error: {response}")
                        error_count += 1
                        # Don't delete on error - will retry next time

                except Exception as e:
                    print(f"[GET_STATS] Failed to invoke PutStatsLambda: {str(e)}")
                    error_count += 1
                    import traceback
                    traceback.print_exc()
                    # Don't delete on error - will retry next time

            except Exception as e:
                print(f"[GET_STATS] Error processing {queue_key}: {str(e)}")
                error_count += 1
                import traceback
                traceback.print_exc()
                # Continue to next entry even if one fails

        print(f"[GET_STATS] Summary - Processed: {processed_count}, Errors: {error_count}")
        return {
            "status": "success",
            "processed_count": processed_count,
            "error_count": error_count
        }

    except Exception as e:
        print(f"[GET_STATS] Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "error": str(e)
        }
