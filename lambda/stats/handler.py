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


def aggregate_hashtags_from_raw_feed(bucket):
    """
    Aggregate hashtags from feed:raw:jp:v1 in Valkey.
    Returns top 10 hashtags with counts.
    """
    try:
        if not valkey_client:
            print("[HASHTAGS] Valkey client not available, skipping")
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


def save_trends_current(bucket, top_hashtags):
    """Save top hashtags to stats/trends/trends-current.json"""
    try:
        trends_data = {
            "timestamp": get_jst_now().strftime("%Y-%m-%d %H:%M:%S"),
            "top_hashtags": top_hashtags if top_hashtags else []
        }

        s3_key = "stats/trends/trends-current.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(trends_data, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[TRENDS] Saved trends to {s3_key}")
        return f"s3://{bucket}/{s3_key}"

    except Exception as e:
        print(f"[TRENDS] Error saving trends: {str(e)}")
        return None


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

        execution_time = batch_stats.get("execution_time", get_jst_now().strftime("%Y-%m-%d %H:%M:%S"))
        timestamp = batch_stats.get("timestamp", get_jst_now().strftime("%Y%m%d_%H%M%S"))

        # Step 1: Store batch result
        batch_url = save_batch_stats(bucket, timestamp, batch_stats)
        print(f"[STATS] Saved batch stats to {batch_url}")

        # Step 2: Check if previous day's data needs to be backfilled
        # (If daily file was missing and needs to be recreated from batch files)
        try:
            from datetime import timedelta
            now = get_jst_now()
            yesterday = now - timedelta(days=1)
            yesterday_date = yesterday.strftime("%Y-%m-%d")
            daily_key = f"stats/daily/stats-{yesterday_date}.json"
            print(f"[BACKFILL] Checking for yesterday ({yesterday_date}): {daily_key}")

            # Check if yesterday's daily file exists
            yesterday_exists = False
            try:
                s3_client.head_object(Bucket=bucket, Key=daily_key)
                yesterday_exists = True
            except Exception as e:
                # File doesn't exist, which is expected for backfill
                pass

            if not yesterday_exists:
                print(f"[BACKFILL] Yesterday's daily file missing, attempting to backfill from batch files")
                yesterday_batches = list_batch_files_for_date(bucket, yesterday_date)
                print(f"[BACKFILL] Found {len(yesterday_batches)} batch files")

                if yesterday_batches:
                    backfilled_entry = aggregate_batch_files_for_date(bucket, yesterday_date, yesterday_batches)
                    if backfilled_entry:
                        # Save daily file for yesterday
                        s3_client.put_object(
                            Bucket=bucket,
                            Key=daily_key,
                            Body=json.dumps(backfilled_entry, ensure_ascii=False, indent=2),
                            ContentType="application/json; charset=utf-8"
                        )
                        print(f"[BACKFILL] Backfilled yesterday's data from {len(yesterday_batches)} batch files")

                        # Update dashboard.json immediately with new daily entry
                        dashboard_key = "stats/summary/dashboard.json"
                        try:
                            response = s3_client.get_object(Bucket=bucket, Key=dashboard_key)
                            dashboard_data = json.loads(response["Body"].read().decode("utf-8"))
                        except s3_client.exceptions.NoSuchKey:
                            dashboard_data = {
                                "generated_at": get_jst_now().strftime("%Y-%m-%d %H:%M:%S"),
                                "latest": None,
                                "daily": []
                            }

                        # Add or update the backfilled entry in daily array
                        dashboard_data["daily"] = [d for d in dashboard_data.get("daily", []) if d.get("date") != yesterday_date]
                        dashboard_data["daily"].append(backfilled_entry)
                        dashboard_data["daily"].sort(key=lambda x: x.get("date", ""))
                        dashboard_data["generated_at"] = get_jst_now().strftime("%Y-%m-%d %H:%M:%S")
                        dashboard_data["record_count"] = len(dashboard_data["daily"])

                        s3_client.put_object(
                            Bucket=bucket,
                            Key=dashboard_key,
                            Body=json.dumps(dashboard_data, ensure_ascii=False, indent=2),
                            ContentType="application/json; charset=utf-8"
                        )
                        print(f"[BACKFILL] Updated dashboard.json with backfilled data")
        except Exception as e:
            print(f"[BACKFILL] Warning: Backfill check failed (non-critical): {str(e)}")

        # Step 3: Aggregate hashtags from raw feed
        top_hashtags = aggregate_hashtags_from_raw_feed(bucket)
        trends_url = None
        if top_hashtags:
            trends_url = save_trends_current(bucket, top_hashtags)
            print(f"[STATS] Saved trends: {trends_url}")
        else:
            print("[STATS] No hashtags to save")

        # Step 4: Create dashboard summary (完全に集計された日のみ含む)
        summary_url = create_dashboard_summary(bucket)
        print(f"[STATS] Created dashboard summary: {summary_url}")

        return {
            "status": "success",
            "batch_url": batch_url,
            "trends_url": trends_url,
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
    """Store raw batch statistics to S3 and update dashboard latest section"""

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

        print(f"[BATCH] Saved batch stats to {s3_key}")

        # Update dashboard.json latest section
        dashboard_key = "stats/summary/dashboard.json"
        try:
            response = s3_client.get_object(Bucket=bucket, Key=dashboard_key)
            dashboard_data = json.loads(response["Body"].read().decode("utf-8"))
        except s3_client.exceptions.NoSuchKey:
            # Create new dashboard if doesn't exist
            dashboard_data = {
                "generated_at": get_jst_now().strftime("%Y-%m-%d %H:%M:%S"),
                "latest": batch_stats,
                "daily": []
            }

        # Update latest section
        dashboard_data["generated_at"] = get_jst_now().strftime("%Y-%m-%d %H:%M:%S")
        dashboard_data["latest"] = batch_stats

        s3_client.put_object(
            Bucket=bucket,
            Key=dashboard_key,
            Body=json.dumps(dashboard_data, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[BATCH] Updated dashboard.json latest section")

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
        },
        "getfeed_stats": {
            "total_invocations": None
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

        # Query GetFeed invocations from CloudWatch Metrics
        getfeed_function_name = os.environ.get('GETFEED_FUNCTION_NAME')
        if getfeed_function_name:
            invocations = get_getfeed_invocations(getfeed_function_name, target_date)
            aggregated["getfeed_stats"]["total_invocations"] = invocations
        else:
            print(f"[GETFEED] GETFEED_FUNCTION_NAME not set, skipping invocation tracking")

        print(f"[AGGREGATE] Aggregated {len(batch_files)} files for {target_date}")
        return aggregated
    except Exception as e:
        print(f"[AGGREGATE] Error aggregating batch files: {str(e)}")
        return None




def get_getfeed_invocations(function_name, target_date):
    """
    Query CloudWatch Metrics for GetFeed Lambda invocations for a specific date.

    Args:
        function_name: Lambda function name
        target_date: Date string in format YYYY-MM-DD

    Returns:
        Total invocations for the day, or None if query fails
    """
    try:
        # Parse target date
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        start_time = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)

        # Query CloudWatch Metrics for Invocations
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Invocations',
            Dimensions=[
                {'Name': 'FunctionName', 'Value': function_name}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,  # 1 day in seconds
            Statistics=['Sum']
        )

        if response['Datapoints']:
            total = sum(point['Sum'] for point in response['Datapoints'])
            print(f"[GETFEED] Found {int(total)} invocations for {target_date}")
            return int(total)
        else:
            print(f"[GETFEED] No invocations found for {target_date}")
            return 0

    except Exception as e:
        print(f"[GETFEED] Error querying invocations: {str(e)}")
        return None


def create_dashboard_summary(bucket):
    """
    Update dashboard summary with yesterday's daily aggregation only.
    Keep existing daily data and latest section.
    """
    dashboard_key = "stats/summary/dashboard.json"

    # Calculate yesterday's date
    now = get_jst_now()
    yesterday = now - timedelta(days=1)
    yesterday_date = yesterday.strftime("%Y-%m-%d")
    yesterday_daily_key = f"stats/daily/stats-{yesterday_date}.json"

    print(f"[DASHBOARD] Updating dashboard for yesterday: {yesterday_date}")

    # Load yesterday's daily file if it exists
    yesterday_entry = None
    try:
        response = s3_client.get_object(Bucket=bucket, Key=yesterday_daily_key)
        yesterday_entry = json.loads(response["Body"].read().decode("utf-8"))
        print(f"[DASHBOARD] Loaded yesterday's daily data: {yesterday_daily_key}")
    except s3_client.exceptions.NoSuchKey:
        print(f"[DASHBOARD] Yesterday's daily file not found: {yesterday_daily_key}")
    except Exception as e:
        print(f"[DASHBOARD] Error loading yesterday's daily: {str(e)}")

    # Load existing dashboard to preserve data
    existing_daily = []
    latest_batch = None
    try:
        response = s3_client.get_object(Bucket=bucket, Key=dashboard_key)
        existing_dashboard = json.loads(response["Body"].read().decode("utf-8"))
        existing_daily = existing_dashboard.get("daily", [])
        latest_batch = existing_dashboard.get("latest")
        print(f"[DASHBOARD] Loaded existing dashboard with {len(existing_daily)} daily entries")
    except s3_client.exceptions.NoSuchKey:
        print(f"[DASHBOARD] Dashboard file not found, creating new")
    except Exception as e:
        print(f"[DASHBOARD] Error loading existing dashboard: {str(e)}")

    # Update daily data: replace or add yesterday's entry
    if yesterday_entry:
        # Remove existing entry for yesterday if any
        existing_daily = [d for d in existing_daily if d.get("date") != yesterday_date]
        # Add yesterday's entry
        existing_daily.append(yesterday_entry)
        # Sort by date
        existing_daily.sort(key=lambda x: x.get("date", ""))
        print(f"[DASHBOARD] Updated daily data for {yesterday_date}")

    # Create updated dashboard summary
    dashboard_summary = {
        "generated_at": get_jst_now().strftime("%Y-%m-%d %H:%M:%S"),
        "latest": latest_batch,
        "daily": existing_daily,
        "record_count": len(existing_daily)
    }

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=dashboard_key,
            Body=json.dumps(dashboard_summary, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[DASHBOARD] Updated dashboard summary with {len(existing_daily)} daily records")
        return f"s3://{bucket}/{dashboard_key}"
    except Exception as e:
        print(f"[DASHBOARD] Error saving dashboard summary: {str(e)}")
        raise
