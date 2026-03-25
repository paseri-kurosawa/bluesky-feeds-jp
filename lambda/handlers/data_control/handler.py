import os
import json
import redis
import time
import boto3
import unicodedata
from datetime import datetime, timedelta

# === Configuration ===
DEBUG = os.environ.get("DEBUG", "False").lower() == "true"

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

# === AWS Clients ===
VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "localhost")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
STATISTICS_BUCKET = os.environ.get("STATISTICS_BUCKET", "")
MAX_ITEMS_RAW = 5000
MAX_ITEMS_DENSE = 2000
MAX_ITEMS_STABLETAG = 2000

s3_client = boto3.client("s3")
cloudwatch_client = boto3.client("cloudwatch")

r = redis.Redis(
    host=VALKEY_ENDPOINT,
    port=6379,
    ssl=True,
    ssl_cert_reqs="required",
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)

def get_jst_now():
    """Get current time in JST (UTC+9)"""
    return datetime.utcnow() + timedelta(hours=9)


def get_getfeed_invocations_for_date(target_date):
    """
    Get total GetFeedLambda invocations for a specific date from CloudWatch.

    Args:
        target_date: Date string in format YYYY-MM-DD

    Returns:
        Total invocation count for the date
    """
    try:
        # Parse target date
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")

        # CloudWatch query period: entire day in JST (00:00 - 23:59:59)
        start_time = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)

        # Convert to UTC (JST is UTC+9)
        start_time_utc = start_time - timedelta(hours=9)
        end_time_utc = end_time - timedelta(hours=9)

        print(f"[CLOUDWATCH] Querying GetFeedLambda invocations for {target_date}")
        print(f"  JST: {start_time} - {end_time}")
        print(f"  UTC: {start_time_utc} - {end_time_utc}")

        # Query CloudWatch Metrics for GetFeedLambda Invocations
        # Use 1-hour period and sum across all hours in the day
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Invocations',
            Dimensions=[
                {
                    'Name': 'FunctionName',
                    'Value': 'BlueskyFeedJpStack-GetFeedLambda76B14ED4-DfIhJgHN7YXZ'
                }
            ],
            StartTime=start_time_utc,
            EndTime=end_time_utc,
            Period=3600,  # 1 hour period (CloudWatch stores hourly data)
            Statistics=['Sum']
        )

        total_invocations = 0
        if response['Datapoints']:
            for dp in response['Datapoints']:
                total_invocations += dp.get('Sum', 0)
            print(f"[CLOUDWATCH] Found {len(response['Datapoints'])} hourly datapoints")

        print(f"[CLOUDWATCH] Total invocations for {target_date}: {total_invocations}")
        return int(total_invocations)

    except Exception as e:
        print(f"[CLOUDWATCH] Error fetching invocations for {target_date}: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0


# === Daily Aggregation Helper Functions ===
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


def aggregate_batch_files_for_date(bucket, target_date, batch_files, getfeed_stats=None):
    """
    Aggregate multiple batch files for a specific date

    Args:
        bucket: S3 bucket name
        target_date: Date string in format YYYY-MM-DD
        batch_files: List of S3 keys to aggregate
        getfeed_stats: CloudWatch stats from IngestLambda (optional)

    Returns:
        Aggregated stats dict
    """
    if getfeed_stats is None:
        getfeed_stats = {"total_invocations": 0}
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
        "getfeed_stats": getfeed_stats,
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

        return aggregated
    except Exception as e:
        return None


# === Save Badword Texts to S3 ===
def save_badword_texts_to_s3(bucket, dense_texts, dense_base_forms):
    """Save badword analysis texts (from IngestLambda) to S3"""
    if not dense_texts:
        print("[BADWORD] No dense texts to save")
        return

    try:
        now_jst = get_jst_now()
        timestamp = now_jst.strftime("%Y%m%d_%H%M%S")

        # Save raw texts
        if dense_texts:
            s3_key = f"badword-analysis/dense_posts_{timestamp}.txt"
            content = "\n".join(dense_texts)

            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=content.encode("utf-8"),
                ContentType="text/plain; charset=utf-8",
            )
            print(f"[BADWORD] Saved {len(dense_texts)} dense post texts to s3://{bucket}/{s3_key}")

        # Save base forms
        if dense_base_forms:
            s3_base_forms_key = f"badword-analysis/dense_posts_base_forms_{timestamp}.txt"
            base_forms_content = "\n".join(dense_base_forms)

            s3_client.put_object(
                Bucket=bucket,
                Key=s3_base_forms_key,
                Body=base_forms_content.encode("utf-8"),
                ContentType="text/plain; charset=utf-8",
            )
            print(f"[BADWORD] Saved {len(dense_base_forms)} base forms to s3://{bucket}/{s3_base_forms_key}")

    except Exception as e:
        print(f"[BADWORD ERROR] Failed to save texts: {str(e)}")
        raise


def backfill_previous_day(bucket, getfeed_stats):
    """
    Check if previous day's daily stats exist. If not, backfill from batch files.
    Updates dashboard.json with new daily entry.
    """
    try:
        now = get_jst_now()
        yesterday = now - timedelta(days=1)
        yesterday_date = yesterday.strftime("%Y-%m-%d")
        daily_key = f"stats/daily/stats-{yesterday_date}.json"

        # Check if yesterday's daily file exists
        yesterday_exists = False
        try:
            s3_client.head_object(Bucket=bucket, Key=daily_key)
            yesterday_exists = True
        except Exception as e:
            pass

        if not yesterday_exists:
            yesterday_batches = list_batch_files_for_date(bucket, yesterday_date)

            if yesterday_batches:
                backfilled_entry = aggregate_batch_files_for_date(bucket, yesterday_date, yesterday_batches, getfeed_stats)
                if backfilled_entry:
                    # Save daily file for yesterday
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=daily_key,
                        Body=json.dumps(backfilled_entry, ensure_ascii=False, indent=2),
                        ContentType="application/json; charset=utf-8"
                    )

                    # Update dashboard.json with new daily entry
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

                    s3_client.put_object(
                        Bucket=bucket,
                        Key=dashboard_key,
                        Body=json.dumps(dashboard_data, ensure_ascii=False, indent=2),
                        ContentType="application/json; charset=utf-8"
                    )

                    # Save processing_trends to components/processing_trends.json
                    processing_trends_key = "components/processing_trends.json"
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=processing_trends_key,
                        Body=json.dumps(dashboard_data.get("daily", []), ensure_ascii=False, indent=2),
                        ContentType="application/json; charset=utf-8"
                    )
                    print(f"[S3] Updated processing_trends to {processing_trends_key}")
    except Exception as e:
        pass


# === Responsibility 1: Store Feeds to Valkey ===
def store_feeds(items_raw, items_stablehashtag, batch_spread_seconds):
    """
    Store posts to Valkey feed:raw, feed:dense, and feed:stablehashtag ZSETs.

    Args:
        items_raw: Posts from lang:ja query
        items_stablehashtag: Posts from lang:ja #<tag> query
        batch_spread_seconds: Window for distributing visible_ts

    Returns: (raw_stored, dense_stored, stablehashtag_stored)
    Raises: Exception on critical failure
    """
    r.ping()

    now = int(time.time())
    raw_stored = 0
    dense_stored = 0
    stablehashtag_stored = 0

    # === Store Raw Feed ===
    items_count = len(items_raw)
    for idx, item in enumerate(items_raw):
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

        # Calculate visible_ts: distribute posts across batch_spread_seconds window
        if items_count > 1:
            offset = (idx / (items_count - 1)) * batch_spread_seconds
        else:
            offset = 0
        visible_ts = now + offset

        # Create member as JSON with metadata
        member = json.dumps({
            "uri": uri,
            "ts": ts,
            "visible_ts": visible_ts,
            "density_score": density_score,
            "hashtags": item.get("hashtags", [])
        }, ensure_ascii=False)

        # Store in raw feed
        try:
            result_raw = r.zadd("feed:raw:jp:v1", {member: visible_ts})
            raw_stored += 1
            if result_raw == 0:
                print(f"[WARN] Raw zadd returned 0 (duplicate?): {uri}")
        except Exception as e:
            print(f"[ERROR] Raw zadd failed for {uri}: {e}")
            raise

        # Store in dense feed if score >= threshold
        if density_score >= get_density_threshold():
            try:
                result_dense = r.zadd("feed:dense:jp:v1", {member: visible_ts})
                dense_stored += 1
                if result_dense == 0:
                    print(f"[WARN] Dense zadd returned 0 (duplicate?): {uri}")
            except Exception as e:
                print(f"[ERROR] Dense zadd failed for {uri}: {e}")
                raise

    # === Store StableTag Feed ===
    stablehashtag_count = len(items_stablehashtag)
    for idx, item in enumerate(items_stablehashtag):
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

        # Calculate visible_ts: distribute posts across batch_spread_seconds window for stablehashtag
        if stablehashtag_count > 1:
            offset = (idx / (stablehashtag_count - 1)) * batch_spread_seconds
        else:
            offset = 0
        visible_ts = now + offset

        # Create member as JSON with metadata
        member = json.dumps({
            "uri": uri,
            "ts": ts,
            "visible_ts": visible_ts,
            "density_score": density_score,
            "hashtags": item.get("hashtags", [])
        }, ensure_ascii=False)

        # Store in stablehashtag feed
        try:
            result_stablehashtag = r.zadd("feed:stablehashtag:jp:v1", {member: visible_ts})
            stablehashtag_stored += 1
            if result_stablehashtag == 0:
                print(f"[WARN] StableTag zadd returned 0 (duplicate?): {uri}")
        except Exception as e:
            print(f"[ERROR] StableTag zadd failed for {uri}: {e}")
            raise

    # Trim all feeds to their limits (keep latest)
    r.zremrangebyrank("feed:raw:jp:v1", 0, -MAX_ITEMS_RAW - 1)
    r.zremrangebyrank("feed:dense:jp:v1", 0, -MAX_ITEMS_DENSE - 1)
    r.zremrangebyrank("feed:stablehashtag:jp:v1", 0, -MAX_ITEMS_STABLETAG - 1)

    raw_zcard = r.zcard("feed:raw:jp:v1")
    dense_zcard = r.zcard("feed:dense:jp:v1")
    stablehashtag_zcard = r.zcard("feed:stablehashtag:jp:v1")
    print(f"[STORE] Stored - Raw: {raw_stored}, Dense: {dense_stored}, StableTag: {stablehashtag_stored}")
    print(f"[STORE] Final - Raw ZCARD: {raw_zcard}, Dense ZCARD: {dense_zcard}, StableTag ZCARD: {stablehashtag_zcard}")

    return raw_stored, dense_stored, stablehashtag_stored


# === Responsibility 2: Aggregate Statistics ===
def aggregate_stats(batch_stats):
    """
    Aggregate hashtags and prepare batch statistics.

    Returns: (top_hashtags, enriched_batch_stats)
    Returns: ([], batch_stats) on failure (non-critical)
    """
    top_hashtags = []

    try:
        # Get all posts from raw feed
        raw_posts = r.zrevrange("feed:raw:jp:v1", 0, -1)
        print(f"[STATS] Retrieved {len(raw_posts)} posts (all) from feed:raw:jp:v1")

        if raw_posts:
            # Helper function to aggregate hashtags
            def compute_hashtag_trends(posts, label="ALL"):
                hashtag_counts = {}
                for post_json in posts:
                    try:
                        post_data = json.loads(post_json)
                        hashtags = post_data.get("hashtags", [])
                        for tag in hashtags:
                            normalized_tag = unicodedata.normalize("NFC", tag).lower()
                            hashtag_counts[normalized_tag] = hashtag_counts.get(normalized_tag, 0) + 1
                    except Exception as e:
                        print(f"[STATS] Error parsing post JSON: {str(e)}")
                        continue

                # Sort by count and get top 10
                sorted_hashtags = sorted(
                    hashtag_counts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:10]

                # Assign ranks: same count = same rank
                trends = []
                rank = 1
                prev_count = None
                for i, (tag, count) in enumerate(sorted_hashtags):
                    if prev_count is not None and count < prev_count:
                        rank = i + 1
                    trends.append({"rank": rank, "tag": tag, "count": count})
                    prev_count = count

                print(f"[STATS] Aggregated {len(hashtag_counts)} unique hashtags ({label}), top 10 extracted")
                for ht in trends:
                    print(f"  - #{ht['tag']}: {ht['count']} ({label})")
                return trends

            # Filter posts from last 1 hour (3600 seconds)
            now = int(time.time())
            one_hour_ago = now - 3600
            recent_posts = []
            for post_json in raw_posts:
                try:
                    post_data = json.loads(post_json)
                    post_ts = post_data.get("ts", 0)
                    if post_ts >= one_hour_ago:
                        recent_posts.append(post_json)
                except Exception as e:
                    continue

            print(f"[STATS] Filtered {len(recent_posts)} posts from last 1 hour")

            # Compute trends for both ALL and 1H
            top_hashtags = compute_hashtag_trends(raw_posts, "ALL")
            top_hashtags_1h = compute_hashtag_trends(recent_posts, "1H")
        else:
            print("[STATS] No posts found in feed:raw:jp:v1")
            top_hashtags = []
            top_hashtags_1h = []

    except Exception as e:
        print(f"[STATS] Error aggregating: {str(e)}")
        import traceback
        traceback.print_exc()
        # Continue - aggregation failure is not critical

    # Enrich batch_stats with top_hashtags
    enriched_stats = {
        **batch_stats,
        "top_hashtags": top_hashtags,
        "top_hashtags_1h": top_hashtags_1h
    }

    return top_hashtags, enriched_stats


def extract_stable_hashtags(bucket, days=30, top_n=10):
    """
    Extract TOP stable hashtags from past N days of daily files.
    (Reads from hashtags/daily/ instead of batch/ for TTL consistency)

    Args:
        bucket: S3 bucket name
        days: Number of days to analyze (default 30)
        top_n: Number of top hashtags to return (default 10)

    Returns:
        List of dicts [{"tag": "...", "count": ...}, ...]
    """
    try:
        now = get_jst_now()
        aggregated_all = {}

        # Aggregate daily files for past N days
        for days_ago in range(days, -1, -1):
            date = now - timedelta(days=days_ago)
            date_str = date.strftime("%Y-%m-%d")
            daily_key = f"hashtags/daily/{date_str}.json"

            try:
                response = s3_client.get_object(Bucket=bucket, Key=daily_key)
                daily_data = json.loads(response["Body"].read().decode("utf-8"))

                # Aggregate all hashtags
                for tag, count in daily_data.items():
                    aggregated_all[tag] = aggregated_all.get(tag, 0) + count
            except Exception as e:
                # Daily file may not exist for older dates, continue
                pass

        # Sort by count descending and get top N
        sorted_hashtags = sorted(
            aggregated_all.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]

        return [
            {"tag": tag, "count": count}
            for tag, count in sorted_hashtags
        ]
    except Exception as e:
        print(f"[HASHTAG ERROR] Failed to extract stable hashtags: {str(e)}")
        return []


def save_hashtag_batch(bucket, hashtags):
    """
    Save hashtag batch file to S3.

    Args:
        bucket: S3 bucket name
        hashtags: Dict of {tag: count}
    """
    try:
        now = get_jst_now()
        timestamp = now.strftime("%Y-%m-%d_%H:%M")
        s3_key = f"hashtags/batch/{timestamp}.json"

        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(hashtags, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[HASHTAG] Saved batch to {s3_key}")
    except Exception as e:
        print(f"[HASHTAG ERROR] Failed to save hashtag batch: {str(e)}")


def backfill_hashtag_daily(bucket):
    """
    Check if yesterday's daily file exists. If not, aggregate batch files and create it.
    Also ensures previous day's daily file exists (backfill from batches).

    Args:
        bucket: S3 bucket name
    """
    try:
        now = get_jst_now()
        yesterday = now - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        yesterday_key = f"hashtags/daily/{yesterday_str}.json"

        # Check if yesterday's file exists
        yesterday_exists = False
        try:
            s3_client.head_object(Bucket=bucket, Key=yesterday_key)
            yesterday_exists = True
        except Exception as e:
            pass

        if not yesterday_exists:
            # Yesterday's file is missing - restore from batch files
            prefix = f"hashtags/batch/{yesterday_str}_"
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

            aggregated = {}
            batch_count = 0
            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    key = obj["Key"]
                    response = s3_client.get_object(Bucket=bucket, Key=key)
                    batch_data = json.loads(response["Body"].read().decode("utf-8"))

                    # Aggregate hashtags
                    for tag, count in batch_data.items():
                        aggregated[tag] = aggregated.get(tag, 0) + count

                    batch_count += 1

            if aggregated:
                # Restore yesterday's daily file
                s3_client.put_object(
                    Bucket=bucket,
                    Key=yesterday_key,
                    Body=json.dumps(aggregated, ensure_ascii=False, indent=2),
                    ContentType="application/json; charset=utf-8"
                )
                print(f"[HASHTAG] Restored yesterday's daily file from {batch_count} batches: {yesterday_key}")
            else:
                print(f"[HASHTAG] No batch files found for yesterday")

    except Exception as e:
        print(f"[HASHTAG ERROR] Failed to backfill hashtag daily: {str(e)}")


# === Responsibility 3: Save Statistics to S3 ===
def save_stats_to_s3(batch_stats):
    """
    Save batch statistics to S3 and update dashboard.json.
    Replaces top_hashtags with stable_hashtags (from 30-day analysis).

    Raises: Exception on failure
    """
    if not STATISTICS_BUCKET:
        raise ValueError("STATISTICS_BUCKET environment variable not set")

    timestamp = batch_stats.get("timestamp", get_jst_now().strftime("%Y%m%d_%H%M%S"))
    s3_key = f"stats/batch/stats_{timestamp}.json"

    try:
        # Save batch stats file
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=s3_key,
            Body=json.dumps(batch_stats, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved stats to {s3_key}")

        # Extract stable hashtags for dashboard
        stable_hashtags = extract_stable_hashtags(STATISTICS_BUCKET, days=30, top_n=10)

        # Prepare dashboard stats: remove only top_hashtags (ALL), keep top_hashtags_1h (1H trend), add stable_hashtags
        dashboard_stats = {k: v for k, v in batch_stats.items() if k not in ["top_hashtags"]}
        dashboard_stats["stable_hashtags"] = stable_hashtags

        # Update dashboard.json
        dashboard_key = "stats/summary/dashboard.json"
        try:
            response = s3_client.get_object(Bucket=STATISTICS_BUCKET, Key=dashboard_key)
            dashboard_data = json.loads(response["Body"].read().decode("utf-8"))
        except s3_client.exceptions.NoSuchKey:
            # Create new dashboard if doesn't exist
            dashboard_data = {
                "generated_at": get_jst_now().strftime("%Y-%m-%d %H:%M:%S"),
                "latest": dashboard_stats,
                "daily": []
            }

        # Save latest_report to components/latest_report.json
        latest_report_key = "components/latest_report.json"
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=latest_report_key,
            Body=json.dumps(dashboard_stats, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved latest_report to {latest_report_key}")

        # Save stable_hashtags to components/stable_hashtags.json
        stable_hashtags_key = "components/stable_hashtags.json"
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=stable_hashtags_key,
            Body=json.dumps({
                "generated_at": get_jst_now().strftime("%Y-%m-%d %H:%M:%S"),
                "top_hashtags": stable_hashtags
            }, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved stable_hashtags to {stable_hashtags_key}")

        # Save top_hashtags_1h to components/top_hashtags_1h.json
        top_hashtags_1h_key = "components/top_hashtags_1h.json"
        top_hashtags_1h = dashboard_stats.get("top_hashtags_1h", [])
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=top_hashtags_1h_key,
            Body=json.dumps({
                "generated_at": get_jst_now().strftime("%Y-%m-%d %H:%M:%S"),
                "top_hashtags_1h": top_hashtags_1h
            }, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved top_hashtags_1h to {top_hashtags_1h_key}")

        # Update latest section
        dashboard_data["generated_at"] = get_jst_now().strftime("%Y-%m-%d %H:%M:%S")
        dashboard_data["latest"] = dashboard_stats

        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=dashboard_key,
            Body=json.dumps(dashboard_data, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Updated dashboard.json with latest stats")

        # Save processing_trends to components/processing_trends.json
        processing_trends_key = "components/processing_trends.json"
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=processing_trends_key,
            Body=json.dumps(dashboard_data.get("daily", []), ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Updated processing_trends to {processing_trends_key}")

        return s3_key
    except Exception as e:
        print(f"[S3] Error saving stats or updating dashboard: {str(e)}")
        raise


# === Main Handler ===
def lambda_handler(event, context):
    """
    DataControlLambda: Invoked asynchronously by Ingest Lambda.

    Responsibilities:
    1. Store posts to Valkey (CRITICAL) - raw, dense, and stablehashtag
    2. Aggregate statistics (OPTIONAL)
    3. Save to S3 (OPTIONAL if aggregation succeeds)

    Expected event:
    {
        "items_raw": [...],
        "items_stablehashtag": [...],
        "batch_stats": {...}
    }
    """
    items_raw = event.get("items_raw", [])
    items_stablehashtag = event.get("items_stablehashtag", [])
    batch_stats = event.get("batch_stats", {})
    dense_texts = event.get("dense_texts", [])
    dense_base_forms = event.get("dense_base_forms", [])
    getfeed_stats = event.get("getfeed_stats", {"total_invocations": 0})
    hashtags = event.get("hashtags", {})

    if not items_raw and not items_stablehashtag:
        return {"stored_raw": 0, "stored_dense": 0, "stored_stablehashtag": 0, "note": "no items"}

    # === CRITICAL: Store feeds to Valkey ===
    try:
        batch_spread_seconds = get_batch_spread_seconds()
        raw_stored, dense_stored, stablehashtag_stored = store_feeds(items_raw, items_stablehashtag, batch_spread_seconds)
    except Exception as e:
        return {
            "error": str(e),
            "stored_raw": 0,
            "stored_dense": 0,
            "stored_stablehashtag": 0,
        }

    # === OPTIONAL: Aggregate statistics and save to S3 ===
    s3_saved = False

    # === OPTIONAL: Save batch hashtags ===
    try:
        if hashtags:
            save_hashtag_batch(STATISTICS_BUCKET, hashtags)
    except Exception as e:
        print(f"[OPTIONAL] Hashtag batch save failed (non-critical): {str(e)}")
        import traceback
        traceback.print_exc()

    # === OPTIONAL: Backfill hashtag daily if missing (BEFORE dashboard update) ===
    try:
        backfill_hashtag_daily(STATISTICS_BUCKET)
    except Exception as e:
        print(f"[OPTIONAL] Hashtag daily backfill failed (non-critical): {str(e)}")
        import traceback
        traceback.print_exc()

    # === OPTIONAL: Backfill previous day's daily stats if missing (BEFORE dashboard update) ===
    try:
        backfill_previous_day(STATISTICS_BUCKET, getfeed_stats)
    except Exception as e:
        print(f"[OPTIONAL] Daily backfill failed (non-critical): {str(e)}")
        import traceback
        traceback.print_exc()

    # === Now update dashboard (after all daily files are ready) ===
    if batch_stats:
        try:
            top_hashtags, enriched_stats = aggregate_stats(batch_stats)
            print(f"[PIPELINE] Aggregation complete, saving to S3")

            s3_key = save_stats_to_s3(enriched_stats)
            s3_saved = True

        except Exception as e:
            pass

    # === OPTIONAL: Save badword texts to S3 ===
    try:
        if dense_texts or dense_base_forms:
            save_badword_texts_to_s3(S3_BUCKET, dense_texts, dense_base_forms)
    except Exception as e:
        print(f"[OPTIONAL] Badword text save failed (non-critical): {str(e)}")
        import traceback
        traceback.print_exc()

    # === Return success (feeds were stored) ===
    return {
        "stored_raw": raw_stored,
        "stored_dense": dense_stored,
        "stored_stablehashtag": stablehashtag_stored,
        "stats_saved": s3_saved
    }
