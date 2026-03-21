import json
import os
import boto3

s3_client = boto3.client("s3")


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
        raise


def lambda_handler(event, context):
    """
    PutStatsLambda: Save batch statistics to S3

    Expected event:
    {
        "batch_stats": {...},
        "top_hashtags": [...],
        "queue_key": "stats:queue:TIMESTAMP"
    }
    """

    try:
        bucket = os.environ.get("STATISTICS_BUCKET", "")
        if not bucket:
            raise ValueError("STATISTICS_BUCKET environment variable not set")

        batch_stats = event.get("batch_stats", {})
        top_hashtags = event.get("top_hashtags", [])

        if not batch_stats:
            print("[PUT_STATS] No batch_stats in event")
            return {
                "status": "error",
                "reason": "no_batch_stats"
            }

        # Extract timestamp from batch_stats
        timestamp = batch_stats.get("timestamp", "unknown")

        # Save to S3
        print(f"[PUT_STATS] Saving batch stats to S3 (timestamp: {timestamp})")
        saved_key = save_batch_stats(bucket, timestamp, batch_stats)

        return {
            "status": "success",
            "s3_key": saved_key,
            "timestamp": timestamp
        }

    except Exception as e:
        print(f"[PUT_STATS] Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "error": str(e)
        }
