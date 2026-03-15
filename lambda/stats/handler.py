import json
import os
from datetime import datetime
import boto3

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    """
    StatsLambda: Manages statistics file aggregation

    Responsibilities:
    1. Store batch result: Save raw ingest statistics to batch/
    2. Aggregate daily: Merge with daily/stats-YYYY.json
    3. Create dashboard summary: Aggregate 365 days for dashboard display
    """

    try:
        bucket = os.environ.get("STATISTICS_BUCKET", "")
        if not bucket:
            raise ValueError("STATISTICS_BUCKET environment variable not set")

        # Extract batch stats from event (passed from IngestLambda)
        batch_stats = event.get("batch_stats", {})

        if not batch_stats:
            print("[STATS] No batch stats provided, skipping")
            return {
                "status": "skipped",
                "reason": "no_batch_stats"
            }

        execution_time = batch_stats.get("execution_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        timestamp = batch_stats.get("timestamp", datetime.now().strftime("%Y%m%d_%H%M%S"))

        # Step 1: Store batch result
        batch_url = save_batch_stats(bucket, timestamp, batch_stats)
        print(f"[STATS] Saved batch stats to {batch_url}")

        # Step 2: Aggregate daily stats
        daily_url = aggregate_daily_stats(bucket, execution_time, batch_stats)
        print(f"[STATS] Updated daily stats: {daily_url}")

        # Step 3: Create dashboard summary
        summary_url = create_dashboard_summary(bucket)
        print(f"[STATS] Created dashboard summary: {summary_url}")

        return {
            "status": "success",
            "batch_url": batch_url,
            "daily_url": daily_url,
            "summary_url": summary_url
        }

    except Exception as e:
        print(f"Stats Lambda error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "error": str(e)
        }


def save_batch_stats(bucket, timestamp, batch_stats):
    """Store raw batch statistics to S3"""

    s3_key = f"stats/batch/stats_{timestamp}.json"

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(batch_stats, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        return f"s3://{bucket}/{s3_key}"
    except Exception as e:
        print(f"[BATCH] Error saving batch stats: {str(e)}")
        raise


def aggregate_daily_stats(bucket, execution_time, batch_stats):
    """Merge batch stats into daily aggregation"""

    year = execution_time.split("-")[0]  # Extract YYYY from "2026-03-15 HH:MM:SS"
    date = execution_time.split(" ")[0]   # Extract YYYY-MM-DD

    daily_key = f"stats/daily/stats-{year}.json"

    # Get existing daily stats
    daily_data = []
    try:
        response = s3_client.get_object(Bucket=bucket, Key=daily_key)
        daily_data = json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print(f"[DAILY] Creating new daily stats file: {daily_key}")
        daily_data = []

    # Extract today's entry if exists
    today_entry = next((entry for entry in daily_data if entry.get("date") == date), None)

    # Create new entry from batch stats
    new_entry = {
        "date": date,
        "execution_time": execution_time,
        "badword_analysis": batch_stats.get("badword_analysis", {}),
        "dense_feed": batch_stats.get("dense_feed", {}),
        "processing_summary": batch_stats.get("processing_summary", {})
    }

    # Update or append
    if today_entry:
        idx = daily_data.index(today_entry)
        daily_data[idx] = new_entry
    else:
        daily_data.append(new_entry)

    # Sort by date
    daily_data.sort(key=lambda x: x["date"])

    # Save back to S3
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=daily_key,
            Body=json.dumps(daily_data, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        return f"s3://{bucket}/{daily_key}"
    except Exception as e:
        print(f"[DAILY] Error saving daily stats: {str(e)}")
        raise


def create_dashboard_summary(bucket):
    """Aggregate all daily stats into dashboard format (365 days)"""

    year = datetime.now().strftime("%Y")
    daily_key = f"stats/daily/stats-{year}.json"

    # Get daily stats
    daily_data = []
    try:
        response = s3_client.get_object(Bucket=bucket, Key=daily_key)
        daily_data = json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print(f"[DASHBOARD] No daily stats found, skipping dashboard summary")
        return None

    # Create dashboard summary (just pass through the daily data for now)
    # Can be extended to aggregate multiple years, compute rolling averages, etc.
    dashboard_summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": daily_data,
        "record_count": len(daily_data)
    }

    dashboard_key = "stats/summary/dashboard.json"

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=dashboard_key,
            Body=json.dumps(dashboard_summary, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        return f"s3://{bucket}/{dashboard_key}"
    except Exception as e:
        print(f"[DASHBOARD] Error saving dashboard summary: {str(e)}")
        raise
