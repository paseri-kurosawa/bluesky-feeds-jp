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

def get_getfeed_function_name():
    """Get GetFeed Lambda function name from config.json"""
    config = get_config()
    return config["aws_lambda"]["getfeed_function_name"]

# === AWS Clients ===
VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "localhost")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
STATISTICS_BUCKET = os.environ.get("STATISTICS_BUCKET", "")
MAX_ITEMS_RAW = 2000
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
        getfeed_function_name = get_getfeed_function_name()
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Invocations',
            Dimensions=[
                {
                    'Name': 'FunctionName',
                    'Value': getfeed_function_name
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
    Saves to stats/daily/raw-dense/ and stats/daily/stablehashtag/.
    Updates components/processing_trends_raw-dense.json and processing_trends_stablehashtag.json.
    """
    try:
        now = get_jst_now()
        yesterday = now - timedelta(days=1)
        yesterday_date = yesterday.strftime("%Y-%m-%d")

        # === Process QUERY 1 (raw-dense) ===
        daily_key_raw = f"stats/daily/raw-dense/stats-{yesterday_date}.json"
        yesterday_exists_raw = False
        try:
            s3_client.head_object(Bucket=bucket, Key=daily_key_raw)
            yesterday_exists_raw = True
        except:
            pass

        if not yesterday_exists_raw:
            # List batch files in stats/batch/raw-dense/
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=f"stats/batch/raw-dense/stats_{yesterday_date.replace('-', '')}")
            yesterday_batches_raw = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.json'):
                            yesterday_batches_raw.append(obj['Key'])

            if yesterday_batches_raw:
                backfilled_entry_raw = aggregate_batch_files_for_date(bucket, yesterday_date, yesterday_batches_raw, getfeed_stats)
                if backfilled_entry_raw:
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=daily_key_raw,
                        Body=json.dumps(backfilled_entry_raw, ensure_ascii=False, indent=2),
                        ContentType="application/json; charset=utf-8"
                    )
                    print(f"[S3] Saved backfilled daily stats (raw-dense) to {daily_key_raw}")

        # === Process QUERY 2 (stablehashtag) ===
        daily_key_stablehashtag = f"stats/daily/stablehashtag/stats-{yesterday_date}.json"
        yesterday_exists_stablehashtag = False
        try:
            s3_client.head_object(Bucket=bucket, Key=daily_key_stablehashtag)
            yesterday_exists_stablehashtag = True
        except:
            pass

        if not yesterday_exists_stablehashtag:
            # List batch files in stats/batch/stablehashtag/
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=f"stats/batch/stablehashtag/stats_{yesterday_date.replace('-', '')}")
            yesterday_batches_stablehashtag = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.json'):
                            yesterday_batches_stablehashtag.append(obj['Key'])

            if yesterday_batches_stablehashtag:
                backfilled_entry_stablehashtag = aggregate_batch_files_for_date(bucket, yesterday_date, yesterday_batches_stablehashtag, getfeed_stats)
                if backfilled_entry_stablehashtag:
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=daily_key_stablehashtag,
                        Body=json.dumps(backfilled_entry_stablehashtag, ensure_ascii=False, indent=2),
                        ContentType="application/json; charset=utf-8"
                    )
                    print(f"[S3] Saved backfilled daily stats (stablehashtag) to {daily_key_stablehashtag}")

        # === Update components/processing_trends_raw-dense.json ===
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix="stats/daily/raw-dense/")
        daily_entries_raw = []
        for page in pages:
            if 'Contents' in page:
                for obj in sorted(page['Contents'], key=lambda x: x['Key']):
                    if obj['Key'].endswith('.json'):
                        try:
                            response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                            daily_data = json.loads(response["Body"].read().decode("utf-8"))
                            daily_entries_raw.append(daily_data)
                        except Exception as e:
                            print(f"[WARN] Failed to read {obj['Key']}: {str(e)}")

        processing_trends_raw_key = "components/processing_trends_raw-dense.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=processing_trends_raw_key,
            Body=json.dumps(daily_entries_raw, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Updated processing_trends_raw-dense to {processing_trends_raw_key} with {len(daily_entries_raw)} entries")

        # === Update components/processing_trends_stablehashtag.json ===
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix="stats/daily/stablehashtag/")
        daily_entries_stablehashtag = []
        for page in pages:
            if 'Contents' in page:
                for obj in sorted(page['Contents'], key=lambda x: x['Key']):
                    if obj['Key'].endswith('.json'):
                        try:
                            response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                            daily_data = json.loads(response["Body"].read().decode("utf-8"))
                            daily_entries_stablehashtag.append(daily_data)
                        except Exception as e:
                            print(f"[WARN] Failed to read {obj['Key']}: {str(e)}")

        processing_trends_stablehashtag_key = "components/processing_trends_stablehashtag.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=processing_trends_stablehashtag_key,
            Body=json.dumps(daily_entries_stablehashtag, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Updated processing_trends_stablehashtag to {processing_trends_stablehashtag_key} with {len(daily_entries_stablehashtag)} entries")

    except Exception as e:
        print(f"[BACKFILL] Error in backfill_previous_day: {str(e)}")
        pass


# === Responsibility 1: Store Feeds to Valkey ===
def store_feeds(items_raw, items_stablehashtag, batch_spread_seconds, top_n):
    """
    Store posts to Valkey feed:raw, feed:dense, and feed:stablehashtag ZSETs.

    Args:
        items_raw: Posts from lang:ja query
        items_stablehashtag: Posts from lang:ja #<tag> query
        batch_spread_seconds: Window for distributing visible_ts
        top_n: Number of hashtags in rotation (only applied to stablehashtag feed)

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
        # Reverse order: older posts appear first (idx=0 = oldest), newer posts later (idx=max = newest)
        if items_count > 1:
            reverse_idx = items_count - 1 - idx
            offset = (reverse_idx / (items_count - 1)) * batch_spread_seconds
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

        # Calculate visible_ts: distribute posts with early concentration, sparse later
        # Using exponential distribution: (1 - (1 - reverse_idx/(count-1))^exponent)
        # Reverse order: older posts appear first, newer posts later
        config = get_config()
        spread_exponent = config.get("scheduling", {}).get("stablehashtag_spread_exponent", 0.5)
        if stablehashtag_count > 1:
            reverse_idx = stablehashtag_count - 1 - idx
            offset = batch_spread_seconds * top_n * (1 - (1 - reverse_idx / (stablehashtag_count - 1)) ** spread_exponent)
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


# === Responsibility 2: Aggregate 1H Hashtags (per batch) ===
def aggregate_1h_hashtags(bucket):
    """
    Aggregate hashtags from past 1 hour batch files.
    Reads from hashtags/batch/ and sums counts from files within the last 1 hour.
    Sorts by count (descending).

    Returns: Dict of {tag: count} sorted by count, or {} on failure
    """
    try:
        now = get_jst_now()
        one_hour_ago = now - timedelta(hours=1)
        one_hour_ago_str = one_hour_ago.strftime("%Y-%m-%d_%H:%M")
        now_str = now.strftime("%Y-%m-%d_%H:%M")

        # List batch files from past 1 hour
        prefix = f"hashtags/batch/"
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        aggregated = {}
        batch_count = 0

        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                # Extract timestamp from key: hashtags/batch/YYYY-MM-DD_HH:MM.json
                try:
                    timestamp_str = key.split("/")[-1].replace(".json", "")
                    # Check if within 1 hour range
                    if one_hour_ago_str <= timestamp_str <= now_str:
                        response = s3_client.get_object(Bucket=bucket, Key=key)
                        batch_data = json.loads(response["Body"].read().decode("utf-8"))

                        # Aggregate hashtags
                        for tag, count in batch_data.items():
                            aggregated[tag] = aggregated.get(tag, 0) + count

                        batch_count += 1
                except Exception as e:
                    print(f"[1H HASHTAGS] Error processing {key}: {str(e)}")
                    continue

        # Sort by count (descending)
        sorted_hashtags = sorted(aggregated.items(), key=lambda x: x[1], reverse=True)
        sorted_dict = dict(sorted_hashtags)

        print(f"[1H HASHTAGS] Aggregated {len(aggregated)} unique hashtags from {batch_count} batches (last 1H)")
        for tag, count in sorted_hashtags[:10]:
            print(f"  - #{tag}: {count}")

        return sorted_dict

    except Exception as e:
        print(f"[1H HASHTAGS] Error aggregating: {str(e)}")
        import traceback
        traceback.print_exc()
        return {}


def extract_stable_hashtags(bucket, days=30, top_n=100):
    """
    Extract TOP stable hashtags from past N days of daily files.
    (Reads from hashtags/daily/ instead of batch/ for TTL consistency)

    Args:
        bucket: S3 bucket name
        days: Number of days to analyze (default 30)
        top_n: Number of top hashtags to return (default 100)

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

            # === After backfill, aggregate ALL hashtags (date changed) ===
            try:
                print(f"[ALL HASHTAGS] Starting ALL aggregation (date changed)")
                top_hashtags_all = aggregate_all_hashtags(bucket)
                if top_hashtags_all:
                    print(f"[ALL HASHTAGS] Successfully aggregated {len(top_hashtags_all)} unique hashtags")

                    # === Update rotation/state.json with TOP 100 hashtags ===
                    try:
                        # Convert dict to list format for rotation
                        hashtags_list = [
                            {"tag": tag, "count": count}
                            for tag, count in list(top_hashtags_all.items())[:100]
                        ]

                        rotation_state = {
                            "current_index": 0,
                            "last_rotation_time": get_jst_now().isoformat(),
                            "total_rotations": 0,
                            "stable_hashtags": hashtags_list
                        }

                        state_key = "hashtags/rotation/state.json"
                        s3_client.put_object(
                            Bucket=bucket,
                            Key=state_key,
                            Body=json.dumps(rotation_state, ensure_ascii=False, indent=2),
                            ContentType="application/json; charset=utf-8"
                        )
                        print(f"[ROTATION] Updated rotation/state.json with TOP 100 hashtags")
                    except Exception as e:
                        print(f"[ROTATION] Error updating rotation state: {str(e)}")
                        import traceback
                        traceback.print_exc()

                    # Return the aggregated hashtags for caller to use
                    return top_hashtags_all
                else:
                    print(f"[ALL HASHTAGS] No hashtags aggregated")
            except Exception as e:
                print(f"[ALL HASHTAGS] Error aggregating ALL: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print(f"[HASHTAG] Yesterday's file already exists, skipping backfill and ALL aggregation")

    except Exception as e:
        print(f"[HASHTAG ERROR] Failed to backfill hashtag daily: {str(e)}")


def aggregate_all_hashtags(bucket):
    """
    Aggregate ALL hashtags from daily files.
    Reads from hashtags/daily/ and sums counts from all daily files.
    Sorts by count (descending).

    Returns: Dict of {tag: count} sorted by count, or {} on failure
    """
    try:
        prefix = "hashtags/daily/"
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        aggregated = {}
        daily_count = 0

        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue

                try:
                    response = s3_client.get_object(Bucket=bucket, Key=key)
                    daily_data = json.loads(response["Body"].read().decode("utf-8"))

                    # Aggregate hashtags
                    for tag, count in daily_data.items():
                        aggregated[tag] = aggregated.get(tag, 0) + count

                    daily_count += 1
                except Exception as e:
                    print(f"[ALL HASHTAGS] Error processing {key}: {str(e)}")
                    continue

        # Sort by count (descending)
        sorted_hashtags = sorted(aggregated.items(), key=lambda x: x[1], reverse=True)
        sorted_dict = dict(sorted_hashtags)

        print(f"[ALL HASHTAGS] Aggregated {len(aggregated)} unique hashtags from {daily_count} daily files")
        for tag, count in sorted_hashtags[:10]:
            print(f"  - #{tag}: {count}")

        return sorted_dict

    except Exception as e:
        print(f"[ALL HASHTAGS] Error aggregating: {str(e)}")
        import traceback
        traceback.print_exc()
        return {}


# === Responsibility 3: Save Statistics to S3 ===
def save_stats_to_s3(batch_stats_raw, batch_stats_stablehashtag):
    """
    Save batch statistics to S3 (separated by QUERY type) and update dashboard.
    Replaces top_hashtags with stable_hashtags (from 30-day analysis).

    Raises: Exception on failure
    """
    if not STATISTICS_BUCKET:
        raise ValueError("STATISTICS_BUCKET environment variable not set")

    timestamp_raw = batch_stats_raw.get("timestamp", get_jst_now().strftime("%Y%m%d_%H%M%S"))
    timestamp_stablehashtag = batch_stats_stablehashtag.get("timestamp", get_jst_now().strftime("%Y%m%d_%H%M%S"))

    s3_key_raw = f"stats/batch/raw-dense/stats_{timestamp_raw}.json"
    s3_key_stablehashtag = f"stats/batch/stablehashtag/stats_{timestamp_stablehashtag}.json"

    try:
        # Save QUERY 1 batch stats file
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=s3_key_raw,
            Body=json.dumps(batch_stats_raw, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved raw-dense stats to {s3_key_raw}")

        # Save QUERY 2 batch stats file
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=s3_key_stablehashtag,
            Body=json.dumps(batch_stats_stablehashtag, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved stablehashtag stats to {s3_key_stablehashtag}")

        # Extract stable hashtags for dashboard (TOP 100)
        stable_hashtags = extract_stable_hashtags(STATISTICS_BUCKET, days=30, top_n=100)

        # === Build separate dashboard stats for QUERY 1 and QUERY 2 ===
        # Remove top_hashtags (archive), keep top_hashtags_1h (1H trend), add stable_hashtags
        dashboard_stats_raw = {k: v for k, v in batch_stats_raw.items() if k not in ["top_hashtags"]}
        dashboard_stats_raw["stable_hashtags"] = stable_hashtags

        dashboard_stats_stablehashtag = {k: v for k, v in batch_stats_stablehashtag.items() if k not in ["top_hashtags"]}
        dashboard_stats_stablehashtag["stable_hashtags"] = stable_hashtags

        # Save latest_report_raw-dense to components/latest_report_raw-dense.json
        latest_report_raw_key = "components/latest_report_raw-dense.json"
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=latest_report_raw_key,
            Body=json.dumps(dashboard_stats_raw, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved latest_report_raw-dense to {latest_report_raw_key}")

        # Save latest_report_stablehashtag to components/latest_report_stablehashtag.json
        latest_report_stablehashtag_key = "components/latest_report_stablehashtag.json"
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=latest_report_stablehashtag_key,
            Body=json.dumps(dashboard_stats_stablehashtag, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved latest_report_stablehashtag to {latest_report_stablehashtag_key}")

        # Save stable_hashtags to components/stable_hashtags_from_raw_posts.json
        stable_hashtags_key = "components/stable_hashtags_from_raw_posts.json"
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=stable_hashtags_key,
            Body=json.dumps({
                "generated_at": get_jst_now().strftime("%Y-%m-%d %H:%M:%S"),
                "top_hashtags": stable_hashtags
            }, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved stable_hashtags_from_raw_posts to {stable_hashtags_key}")

        # Save top_hashtags_1h to components/top_hashtags_1h_from_raw_posts.json (QUERY 1 only)
        top_hashtags_1h_key = "components/top_hashtags_1h_from_raw_posts.json"
        top_hashtags_1h_raw = dashboard_stats_raw.get("top_hashtags_1h", {})
        # Convert dict to array format for dashboard
        if isinstance(top_hashtags_1h_raw, dict):
            top_hashtags_1h_array = [{"tag": tag, "count": count} for tag, count in top_hashtags_1h_raw.items()]
        else:
            top_hashtags_1h_array = top_hashtags_1h_raw if isinstance(top_hashtags_1h_raw, list) else []
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=top_hashtags_1h_key,
            Body=json.dumps({
                "generated_at": get_jst_now().strftime("%Y-%m-%d %H:%M:%S"),
                "top_hashtags_1h": top_hashtags_1h_array
            }, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Saved top_hashtags_1h_from_raw_posts to {top_hashtags_1h_key}")

        # === Aggregate daily files (separated by QUERY) ===
        # QUERY 1: Aggregate stats/daily/raw-dense/
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=STATISTICS_BUCKET, Prefix="stats/daily/raw-dense/")
        daily_entries_raw = []
        for page in pages:
            if 'Contents' in page:
                for obj in sorted(page['Contents'], key=lambda x: x['Key']):
                    if obj['Key'].endswith('.json'):
                        try:
                            response = s3_client.get_object(Bucket=STATISTICS_BUCKET, Key=obj['Key'])
                            daily_data = json.loads(response["Body"].read().decode("utf-8"))
                            daily_entries_raw.append(daily_data)
                        except Exception as e:
                            print(f"[WARN] Failed to read {obj['Key']}: {str(e)}")

        processing_trends_raw_key = "components/processing_trends_raw-dense.json"
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=processing_trends_raw_key,
            Body=json.dumps(daily_entries_raw, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Updated processing_trends_raw-dense to {processing_trends_raw_key} with {len(daily_entries_raw)} entries")

        # QUERY 2: Aggregate stats/daily/stablehashtag/
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=STATISTICS_BUCKET, Prefix="stats/daily/stablehashtag/")
        daily_entries_stablehashtag = []
        for page in pages:
            if 'Contents' in page:
                for obj in sorted(page['Contents'], key=lambda x: x['Key']):
                    if obj['Key'].endswith('.json'):
                        try:
                            response = s3_client.get_object(Bucket=STATISTICS_BUCKET, Key=obj['Key'])
                            daily_data = json.loads(response["Body"].read().decode("utf-8"))
                            daily_entries_stablehashtag.append(daily_data)
                        except Exception as e:
                            print(f"[WARN] Failed to read {obj['Key']}: {str(e)}")

        processing_trends_stablehashtag_key = "components/processing_trends_stablehashtag.json"
        s3_client.put_object(
            Bucket=STATISTICS_BUCKET,
            Key=processing_trends_stablehashtag_key,
            Body=json.dumps(daily_entries_stablehashtag, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Updated processing_trends_stablehashtag to {processing_trends_stablehashtag_key} with {len(daily_entries_stablehashtag)} entries")

        return s3_key_raw  # Return raw-dense key as primary
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
    batch_stats_raw = event.get("batch_stats_raw", {})
    batch_stats_stablehashtag = event.get("batch_stats_stablehashtag", {})
    dense_texts = event.get("dense_texts", [])
    dense_texts_stablehashtag = event.get("dense_texts_stablehashtag", [])
    dense_base_forms = event.get("dense_base_forms", [])
    dense_base_forms_stablehashtag = event.get("dense_base_forms_stablehashtag", [])
    getfeed_stats = event.get("getfeed_stats", {"total_invocations": 0})
    hashtags = event.get("hashtags", {})
    top_n = event.get("top_n")

    if not items_raw and not items_stablehashtag:
        return {"stored_raw": 0, "stored_dense": 0, "stored_stablehashtag": 0, "note": "no items"}

    if top_n is None:
        print(f"[ERROR] top_n is missing from event payload, cannot proceed")
        return {"error": "top_n is required", "stored_raw": 0, "stored_dense": 0, "stored_stablehashtag": 0}

    # === CRITICAL: Store feeds to Valkey ===
    try:
        batch_spread_seconds = get_batch_spread_seconds()
        raw_stored, dense_stored, stablehashtag_stored = store_feeds(items_raw, items_stablehashtag, batch_spread_seconds, top_n)
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
    # This also returns ALL hashtags if date changed
    try:
        top_hashtags_all = backfill_hashtag_daily(STATISTICS_BUCKET)
        if top_hashtags_all:
            batch_stats_raw["top_hashtags"] = top_hashtags_all
            batch_stats_stablehashtag["top_hashtags"] = top_hashtags_all
            print(f"[PIPELINE] ALL hashtags updated from backfill")
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

    # === Aggregate 1H hashtags (per batch) ===
    try:
        top_hashtags_1h = aggregate_1h_hashtags(STATISTICS_BUCKET)
        if top_hashtags_1h:
            batch_stats_raw["top_hashtags_1h"] = top_hashtags_1h
            batch_stats_stablehashtag["top_hashtags_1h"] = top_hashtags_1h
    except Exception as e:
        print(f"[OPTIONAL] 1H hashtags aggregation failed (non-critical): {str(e)}")
        import traceback
        traceback.print_exc()

    # === Now update dashboard (after all daily files are ready) ===
    if batch_stats_raw and batch_stats_stablehashtag:
        try:
            # save_stats_to_s3 now accepts both QUERY 1 and QUERY 2 stats
            s3_key_raw = save_stats_to_s3(batch_stats_raw, batch_stats_stablehashtag)
            s3_saved = True
            print(f"[PIPELINE] Statistics saved to S3")

        except Exception as e:
            print(f"[OPTIONAL] Stats save failed (non-critical): {str(e)}")
            import traceback
            traceback.print_exc()

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
