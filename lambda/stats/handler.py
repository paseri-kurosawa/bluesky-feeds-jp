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

        # Step 4: Check if previous day's data needs to be backfilled
        # (If daily file was missing and needs to be recreated from batch files)
        try:
            from datetime import timedelta
            now = datetime.now()
            yesterday = now - timedelta(days=1)
            yesterday_date = yesterday.strftime("%Y-%m-%d")
            year = yesterday.strftime("%Y")
            daily_key = f"stats/daily/stats-{year}.json"

            # Check if yesterday's entry exists in daily
            daily_data = []
            try:
                response = s3_client.get_object(Bucket=bucket, Key=daily_key)
                daily_data = json.loads(response["Body"].read().decode("utf-8"))
            except s3_client.exceptions.NoSuchKey:
                pass

            yesterday_exists = any(entry.get("date") == yesterday_date for entry in daily_data)

            if not yesterday_exists:
                print(f"[BACKFILL] Yesterday's data missing, attempting to backfill from batch files")
                yesterday_batches = list_batch_files_for_date(bucket, yesterday_date)

                if yesterday_batches:
                    backfilled_entry = aggregate_batch_files_for_date(bucket, yesterday_date, yesterday_batches)
                    if backfilled_entry:
                        daily_data.append(backfilled_entry)
                        daily_data.sort(key=lambda x: x["date"])

                        # Save daily
                        s3_client.put_object(
                            Bucket=bucket,
                            Key=daily_key,
                            Body=json.dumps(daily_data, ensure_ascii=False, indent=2),
                            ContentType="application/json; charset=utf-8"
                        )
                        print(f"[BACKFILL] Backfilled yesterday's data from {len(yesterday_batches)} batch files")

                        # Recreate dashboard summary
                        summary_url = create_dashboard_summary(bucket)
                        print(f"[BACKFILL] Updated dashboard summary after backfill")
        except Exception as e:
            print(f"[BACKFILL] Warning: Backfill check failed (non-critical): {str(e)}")

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
    batch_json = json.dumps(batch_stats, ensure_ascii=False, indent=2).encode("utf-8")

    try:
        # Save timestamped file
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=batch_json,
            ContentType="application/json; charset=utf-8"
        )

        # Also save as latest.json (always overwrite)
        s3_client.put_object(
            Bucket=bucket,
            Key="stats/batch/latest.json",
            Body=batch_json,
            ContentType="application/json; charset=utf-8"
        )

        print(f"[BATCH] Saved batch stats to {s3_key} and stats/batch/latest.json")
        return f"s3://{bucket}/{s3_key}"
    except Exception as e:
        print(f"[BATCH] Error saving batch stats: {str(e)}")
        raise


def list_batch_files_for_date(bucket, target_date):
    """
    List all batch files for a specific date using ListObjectsV2

    Args:
        bucket: S3 bucket name
        target_date: Date string in format YYYY-MM-DD

    Returns:
        List of S3 keys matching the date
    """
    target_yyyymmdd = target_date.replace("-", "")
    prefix = f"stats/batch/stats_{target_yyyymmdd}_"

    matching_files = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    matching_files.append(obj['Key'])

        matching_files.sort()
        print(f"[LIST] Found {len(matching_files)} batch files for {target_date}")
        return matching_files
    except Exception as e:
        print(f"[LIST] Error listing batch files: {str(e)}")
        return []


def aggregate_batch_files_for_date(bucket, target_date, batch_files):
    """
    Aggregate multiple batch files for a specific date

    Args:
        bucket: S3 bucket name
        target_date: Date string in format YYYY-MM-DD
        batch_files: List of S3 keys to aggregate

    Returns:
        Aggregated stats dict
    """
    if not batch_files:
        return None

    aggregated = {
        "date": target_date,
        "execution_time": None,
        "processing_summary": {
            "total_fetched": 0,
            "invalid_fields": 0,
            "moderation_labels": 0,
            "non_japanese": 0,
            "passed_filters": 0,
        },
        "badword_analysis": {
            "posts_with_badwords": 0,
            "total_matches": 0,
        },
        "dense_feed": {
            "total_items": 0,
            "text_only_short": 0,
            "dense_posts": 0,
        }
    }

    try:
        for s3_key in batch_files:
            response = s3_client.get_object(Bucket=bucket, Key=s3_key)
            batch_data = json.loads(response["Body"].read().decode("utf-8"))

            # Update execution_time to latest
            aggregated["execution_time"] = batch_data.get("execution_time")

            # Aggregate processing_summary
            ps = batch_data.get("processing_summary", {})
            aggregated["processing_summary"]["total_fetched"] += ps.get("total_fetched", 0)
            aggregated["processing_summary"]["invalid_fields"] += ps.get("invalid_fields", 0)
            aggregated["processing_summary"]["moderation_labels"] += ps.get("moderation_labels", 0)
            aggregated["processing_summary"]["non_japanese"] += ps.get("non_japanese", 0)
            aggregated["processing_summary"]["passed_filters"] += ps.get("passed_filters", 0)

            # Aggregate badword_analysis
            ba = batch_data.get("badword_analysis", {})
            aggregated["badword_analysis"]["posts_with_badwords"] += ba.get("posts_with_badwords", 0)
            aggregated["badword_analysis"]["total_matches"] += ba.get("total_matches", 0)

            # Aggregate dense_feed
            df = batch_data.get("dense_feed", {})
            aggregated["dense_feed"]["total_items"] += df.get("total_items", 0)
            aggregated["dense_feed"]["text_only_short"] += df.get("text_only_short", 0)
            aggregated["dense_feed"]["dense_posts"] += df.get("dense_posts", 0)

        # Recalculate rates
        total_fetched = aggregated["processing_summary"]["total_fetched"]
        passed_filters = aggregated["processing_summary"]["passed_filters"]
        posts_with_badwords = aggregated["badword_analysis"]["posts_with_badwords"]
        total_matches = aggregated["badword_analysis"]["total_matches"]
        total_items = aggregated["dense_feed"]["total_items"]
        dense_posts = aggregated["dense_feed"]["dense_posts"]

        aggregated["processing_summary"]["rates"] = {
            "invalid_fields_rate": round(aggregated["processing_summary"]["invalid_fields"] / total_fetched * 100, 1) if total_fetched else 0,
            "moderation_labels_rate": round(aggregated["processing_summary"]["moderation_labels"] / total_fetched * 100, 1) if total_fetched else 0,
            "non_japanese_rate": round(aggregated["processing_summary"]["non_japanese"] / total_fetched * 100, 1) if total_fetched else 0,
            "passed_filters_rate": round(passed_filters / total_fetched * 100, 1) if total_fetched else 0,
        }

        aggregated["badword_analysis"]["hit_rate"] = round(posts_with_badwords / passed_filters * 100, 1) if passed_filters else 0
        aggregated["badword_analysis"]["avg_matches_per_hit"] = round(total_matches / posts_with_badwords, 2) if posts_with_badwords > 0 else 0

        aggregated["dense_feed"]["dense_rate"] = round(dense_posts / total_items * 100, 1) if total_items > 0 else 0

        print(f"[AGGREGATE] Aggregated {len(batch_files)} files for {target_date}")
        return aggregated
    except Exception as e:
        print(f"[AGGREGATE] Error aggregating batch files: {str(e)}")
        return None


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

    # Get batch stats data
    batch_processing = batch_stats.get("processing_summary", {})
    batch_badword = batch_stats.get("badword_analysis", {})
    batch_dense = batch_stats.get("dense_feed", {})

    if today_entry:
        # Aggregate: add new batch data to existing daily data
        today_processing = today_entry.get("processing_summary", {})
        today_badword = today_entry.get("badword_analysis", {})
        today_dense = today_entry.get("dense_feed", {})

        # Aggregate processing_summary (add counts)
        aggregated_processing = {
            "total_fetched": today_processing.get("total_fetched", 0) + batch_processing.get("total_fetched", 0),
            "invalid_fields": today_processing.get("invalid_fields", 0) + batch_processing.get("invalid_fields", 0),
            "moderation_labels": today_processing.get("moderation_labels", 0) + batch_processing.get("moderation_labels", 0),
            "non_japanese": today_processing.get("non_japanese", 0) + batch_processing.get("non_japanese", 0),
            "passed_filters": today_processing.get("passed_filters", 0) + batch_processing.get("passed_filters", 0),
        }

        # Recalculate rates based on aggregated totals
        total_fetched = aggregated_processing["total_fetched"]
        passed_filters = aggregated_processing["passed_filters"]

        aggregated_processing["rates"] = {
            "invalid_fields_rate": round(aggregated_processing["invalid_fields"] / total_fetched * 100, 1) if total_fetched else 0,
            "moderation_labels_rate": round(aggregated_processing["moderation_labels"] / total_fetched * 100, 1) if total_fetched else 0,
            "non_japanese_rate": round(aggregated_processing["non_japanese"] / total_fetched * 100, 1) if total_fetched else 0,
            "passed_filters_rate": round(passed_filters / total_fetched * 100, 1) if total_fetched else 0,
        }

        # Aggregate badword_analysis (add counts, no matched_words)
        posts_with_badwords = today_badword.get("posts_with_badwords", 0) + batch_badword.get("posts_with_badwords", 0)
        total_matches = today_badword.get("total_matches", 0) + batch_badword.get("total_matches", 0)

        aggregated_badword = {
            "posts_with_badwords": posts_with_badwords,
            "hit_rate": round(posts_with_badwords / passed_filters * 100, 1) if passed_filters else 0,
            "total_matches": total_matches,
            "avg_matches_per_hit": round(total_matches / posts_with_badwords, 2) if posts_with_badwords > 0 else 0,
        }

        # Aggregate dense_feed (add counts)
        aggregated_dense = {
            "total_items": today_dense.get("total_items", 0) + batch_dense.get("total_items", 0),
            "text_only_short": today_dense.get("text_only_short", 0) + batch_dense.get("text_only_short", 0),
            "dense_posts": today_dense.get("dense_posts", 0) + batch_dense.get("dense_posts", 0),
            "dense_rate": round((today_dense.get("dense_posts", 0) + batch_dense.get("dense_posts", 0)) / (today_dense.get("total_items", 0) + batch_dense.get("total_items", 0)) * 100, 1) if (today_dense.get("total_items", 0) + batch_dense.get("total_items", 0)) > 0 else 0,
        }

        new_entry = {
            "date": date,
            "execution_time": execution_time,
            "processing_summary": aggregated_processing,
            "badword_analysis": aggregated_badword,
            "dense_feed": aggregated_dense,
        }

        idx = daily_data.index(today_entry)
        daily_data[idx] = new_entry
    else:
        # First entry for the day
        total_fetched = batch_processing.get("total_fetched", 0)
        passed_filters = batch_processing.get("passed_filters", 0)
        posts_with_badwords = batch_badword.get("posts_with_badwords", 0)
        total_matches = batch_badword.get("total_matches", 0)
        total_items = batch_dense.get("total_items", 0)
        dense_posts = batch_dense.get("dense_posts", 0)

        new_entry = {
            "date": date,
            "execution_time": execution_time,
            "processing_summary": {
                **batch_processing,
                "rates": {
                    "invalid_fields_rate": round(batch_processing.get("invalid_fields", 0) / total_fetched * 100, 1) if total_fetched else 0,
                    "moderation_labels_rate": round(batch_processing.get("moderation_labels", 0) / total_fetched * 100, 1) if total_fetched else 0,
                    "non_japanese_rate": round(batch_processing.get("non_japanese", 0) / total_fetched * 100, 1) if total_fetched else 0,
                    "passed_filters_rate": round(passed_filters / total_fetched * 100, 1) if total_fetched else 0,
                }
            },
            "badword_analysis": {
                "posts_with_badwords": posts_with_badwords,
                "hit_rate": round(posts_with_badwords / passed_filters * 100, 1) if passed_filters else 0,
                "total_matches": total_matches,
                "avg_matches_per_hit": round(total_matches / posts_with_badwords, 2) if posts_with_badwords > 0 else 0,
            },
            "dense_feed": {
                "total_items": total_items,
                "text_only_short": batch_dense.get("text_only_short", 0),
                "dense_posts": dense_posts,
                "dense_rate": round(dense_posts / total_items * 100, 1) if total_items > 0 else 0,
            },
        }

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
