import os
import json
import redis
import boto3

# Valkey connection
VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "localhost")
STATS_FUNCTION_NAME = os.environ.get("STATS_FUNCTION_NAME", "")

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


def aggregate_hashtags_from_raw_feed():
    """
    Aggregate hashtags from feed:raw:jp:v1 in Valkey.
    Returns top 10 hashtags with counts.
    """
    try:
        if not valkey_client:
            print("[HASHTAGS] Valkey client not available")
            return None

        # Get all posts from feed:raw:jp:v1
        raw_posts = valkey_client.zrange("feed:raw:jp:v1", 0, -1)
        print(f"[HASHTAGS] Retrieved {len(raw_posts)} posts from feed:raw:jp:v1")

        if not raw_posts:
            print("[HASHTAGS] No posts found in feed:raw:jp:v1")
            return None

        # Aggregate hashtags
        hashtag_counts = {}
        for post_json in raw_posts:
            try:
                post_data = json.loads(post_json)
                hashtags = post_data.get("hashtags", [])
                for tag in hashtags:
                    hashtag_counts[tag] = hashtag_counts.get(tag, 0) + 1
            except Exception as e:
                print(f"[HASHTAGS] Error parsing post JSON: {str(e)}")
                continue

        # Sort by count and get top 10
        sorted_hashtags = sorted(
            hashtag_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]

        top_hashtags = [
            {"rank": i + 1, "tag": tag, "count": count}
            for i, (tag, count) in enumerate(sorted_hashtags)
        ]

        print(f"[HASHTAGS] Aggregated {len(hashtag_counts)} unique hashtags, top 10 extracted")
        for ht in top_hashtags:
            print(f"  - #{ht['tag']}: {ht['count']}")

        return top_hashtags

    except Exception as e:
        print(f"[HASHTAGS] Error aggregating hashtags: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def lambda_handler(event, context):
    """
    AggregationLambda: Aggregates hashtags from Valkey and invokes StatsLambda.

    Expected event:
    {
        "batch_stats": {...}
    }
    """
    try:
        # Extract batch_stats from event
        batch_stats = event.get("batch_stats", {})
        print(f"[AGGREGATION] Received event with batch_stats")

        # Aggregate hashtags from Valkey
        top_hashtags = aggregate_hashtags_from_raw_feed()

        # Prepare payload for StatsLambda
        stats_payload = {
            "batch_stats": batch_stats,
            "top_hashtags": top_hashtags if top_hashtags else []
        }

        # Invoke StatsLambda synchronously
        if STATS_FUNCTION_NAME:
            lambda_client = boto3.client("lambda")
            response = lambda_client.invoke(
                FunctionName=STATS_FUNCTION_NAME,
                InvocationType="RequestResponse",  # Synchronous
                Payload=json.dumps(stats_payload),
            )
            print(f"[AGGREGATION] StatsLambda invoked: {response['StatusCode']}")

            # Extract response
            if response['StatusCode'] == 200:
                response_payload = json.loads(response['Payload'].read())
                print(f"[AGGREGATION] StatsLambda response: {response_payload}")
                return {
                    "status": "success",
                    "stats_response": response_payload
                }
            else:
                print(f"[AGGREGATION] StatsLambda error: {response}")
                return {
                    "status": "error",
                    "error": f"StatsLambda returned {response['StatusCode']}"
                }
        else:
            print("[AGGREGATION] STATS_FUNCTION_NAME not set")
            return {
                "status": "error",
                "error": "STATS_FUNCTION_NAME not set"
            }

    except Exception as e:
        print(f"[AGGREGATION] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "error": str(e)
        }
