import os
import json
import redis
import time
import boto3
import unicodedata
from datetime import datetime, timedelta

# === Configuration ===
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
STATISTICS_BUCKET = os.environ.get("STATISTICS_BUCKET", "")
MAX_ITEMS_RAW = 5000
MAX_ITEMS_DENSE = 2000

s3_client = boto3.client("s3")

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


def backfill_previous_day(bucket):
    """
    Check if previous day's daily stats exist. If not, backfill from batch files.
    Updates dashboard.json with new daily entry.
    """
    try:
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
        except Exception:
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
                    print(f"[BACKFILL] Updated dashboard.json with backfilled data")
    except Exception as e:
        print(f"[BACKFILL] Warning: Backfill check failed (non-critical): {str(e)}")


# === Responsibility 1: Store Feeds to Valkey ===
def store_feeds(items, batch_spread_seconds):
    """
    Store posts to Valkey feed:raw and feed:dense ZSETs.

    Returns: (raw_stored, dense_stored)
    Raises: Exception on critical failure
    """
    r.ping()

    now = int(time.time())
    raw_stored = 0
    dense_stored = 0
    items_count = len(items)

    for idx, item in enumerate(items):
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

    # Trim both feeds to their limits (keep latest)
    r.zremrangebyrank("feed:raw:jp:v1", 0, -MAX_ITEMS_RAW - 1)
    r.zremrangebyrank("feed:dense:jp:v1", 0, -MAX_ITEMS_DENSE - 1)

    raw_zcard = r.zcard("feed:raw:jp:v1")
    dense_zcard = r.zcard("feed:dense:jp:v1")
    print(f"[STORE] Stored - Raw: {raw_stored}, Dense: {dense_stored}")
    print(f"[STORE] Final - Raw ZCARD: {raw_zcard}, Dense ZCARD: {dense_zcard}")

    return raw_stored, dense_stored


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
            # Aggregate hashtags
            hashtag_counts = {}
            sample_posts_with_tags = 0
            for idx, post_json in enumerate(raw_posts):
                try:
                    post_data = json.loads(post_json)
                    hashtags = post_data.get("hashtags", [])
                    if hashtags and idx < 5:  # Log first 5 posts with hashtags
                        print(f"[STATS] Sample post {idx}: hashtags={hashtags}")
                        sample_posts_with_tags += 1
                    for tag in hashtags:
                        # Normalize: Unicode NFC + lowercase for case-insensitive grouping
                        normalized_tag = unicodedata.normalize("NFC", tag).lower()
                        hashtag_counts[normalized_tag] = hashtag_counts.get(normalized_tag, 0) + 1
                except Exception as e:
                    print(f"[STATS] Error parsing post JSON: {str(e)}")
                    continue

            print(f"[STATS] Sample posts with tags: {sample_posts_with_tags} / {len(raw_posts)}")

            # Sort by count and get top 10
            sorted_hashtags = sorted(
                hashtag_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]

            # Assign ranks: same count = same rank
            top_hashtags = []
            rank = 1
            prev_count = None
            for i, (tag, count) in enumerate(sorted_hashtags):
                if prev_count is not None and count < prev_count:
                    rank = i + 1
                top_hashtags.append({"rank": rank, "tag": tag, "count": count})
                prev_count = count

            print(f"[STATS] Aggregated {len(hashtag_counts)} unique hashtags, top 10 extracted")
            for ht in top_hashtags:
                print(f"  - #{ht['tag']}: {ht['count']}")
        else:
            print("[STATS] No posts found in feed:raw:jp:v1")

    except Exception as e:
        print(f"[STATS] Error aggregating: {str(e)}")
        import traceback
        traceback.print_exc()
        # Continue - aggregation failure is not critical

    # Enrich batch_stats with top_hashtags
    enriched_stats = {
        **batch_stats,
        "top_hashtags": top_hashtags
    }

    return top_hashtags, enriched_stats


# === Responsibility 3: Save Statistics to S3 ===
def save_stats_to_s3(batch_stats):
    """
    Save batch statistics to S3 and update dashboard.json.

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

        # Update dashboard.json
        dashboard_key = "stats/summary/dashboard.json"
        try:
            response = s3_client.get_object(Bucket=STATISTICS_BUCKET, Key=dashboard_key)
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
            Bucket=STATISTICS_BUCKET,
            Key=dashboard_key,
            Body=json.dumps(dashboard_data, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Updated dashboard.json latest section")

        return s3_key
    except Exception as e:
        print(f"[S3] Error saving stats or updating dashboard: {str(e)}")
        raise


# === Main Handler ===
def lambda_handler(event, context):
    """
    DataControlLambda: Invoked asynchronously by Ingest Lambda.

    Responsibilities:
    1. Store posts to Valkey (CRITICAL)
    2. Aggregate statistics (OPTIONAL)
    3. Save to S3 (OPTIONAL if aggregation succeeds)

    Expected event:
    {
        "items": [...],
        "batch_stats": {...}
    }
    """
    items = event.get("items", [])
    batch_stats = event.get("batch_stats", {})

    if not items:
        return {"stored_raw": 0, "stored_dense": 0, "note": "no items"}

    # === CRITICAL: Store feeds to Valkey ===
    try:
        batch_spread_seconds = get_batch_spread_seconds()
        raw_stored, dense_stored = store_feeds(items, batch_spread_seconds)
    except Exception as e:
        print(f"[CRITICAL] Store feeds failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "error": str(e),
            "stored_raw": 0,
            "stored_dense": 0,
        }

    # === OPTIONAL: Aggregate statistics and save to S3 ===
    s3_saved = False

    if batch_stats:
        try:
            top_hashtags, enriched_stats = aggregate_stats(batch_stats)
            print(f"[PIPELINE] Aggregation complete, saving to S3")

            s3_key = save_stats_to_s3(enriched_stats)
            s3_saved = True
            print(f"[PIPELINE] S3 save successful: {s3_key}")

        except Exception as e:
            print(f"[OPTIONAL] Stats aggregation/save failed (non-critical): {str(e)}")
            import traceback
            traceback.print_exc()
    else:
        print("[OPTIONAL] No batch_stats provided, skipping aggregation")

    # === OPTIONAL: Backfill previous day's daily stats if missing ===
    try:
        backfill_previous_day(STATISTICS_BUCKET)
    except Exception as e:
        print(f"[OPTIONAL] Daily backfill failed (non-critical): {str(e)}")
        import traceback
        traceback.print_exc()

    # === Return success (feeds were stored) ===
    return {
        "stored_raw": raw_stored,
        "stored_dense": dense_stored,
        "stats_saved": s3_saved
    }
