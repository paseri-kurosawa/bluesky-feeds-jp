import os
import json
import time
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta
from density_scorer import calculate_density_score, tokenize_japanese, extract_base_forms

# Japan Standard Time (UTC+9)
JST = timezone(timedelta(hours=9))

# AWS Clients
cloudwatch_client = boto3.client("cloudwatch")

# CloudWatch query function
def get_getfeed_invocations_for_date(target_date):
    """
    Get total GetFeedLambda invocations for a specific date from CloudWatch.

    Args:
        target_date: Date string in format YYYY-MM-DD

    Returns:
        Total invocation count for the date (0 if query fails)
    """
    try:
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        start_time = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)

        # Convert to UTC (JST is UTC+9)
        start_time_utc = start_time - timedelta(hours=9)
        end_time_utc = end_time - timedelta(hours=9)

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
            Period=3600,
            Statistics=['Sum']
        )

        total_invocations = 0
        if response.get('Datapoints'):
            for dp in response['Datapoints']:
                total_invocations += dp.get('Sum', 0)

        return int(total_invocations)

    except Exception as e:
        return 0

# Load configuration
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

# Environment variables
BSKY_SECRET_NAME = os.environ.get("BSKY_SECRET_NAME", "bluesky-feed-jp/credentials")
STORE_FUNCTION_NAME = os.environ.get("STORE_FUNCTION_NAME", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")

# Cached credentials (loaded at runtime from Secrets Manager)
_bsky_credentials = None

def get_bsky_credentials():
    """Load Bluesky credentials from AWS Secrets Manager"""
    global _bsky_credentials
    if _bsky_credentials is None:
        client = boto3.client("secretsmanager")
        try:
            response = client.get_secret_value(SecretId=BSKY_SECRET_NAME)
            secret = json.loads(response["SecretString"])
            _bsky_credentials = {
                "handle": secret.get("handle", ""),
                "appPassword": secret.get("appPassword", ""),
            }
        except Exception as e:
            print(f"[ERROR] Failed to retrieve Bluesky credentials from Secrets Manager: {e}")
            raise
    return _bsky_credentials

# Density threshold loaded from config.json
def get_density_threshold():
    """Get density threshold from config.json"""
    config = get_config()
    return float(config["scoring"]["density_threshold"]["threshold"])

# Language detection model (lazy loaded)
_model = None

def get_language_model():
    """Lazy load fastText model"""
    global _model
    if _model is None:
        import fasttext
        import urllib.request

        # Download model if not present
        model_path = "/tmp/lid.176.ftz"
        if not os.path.exists(model_path):
            print("Downloading fastText language model...")
            urllib.request.urlretrieve(
                "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz",
                model_path
            )
        _model = fasttext.load_model(model_path)
    return _model

def is_japanese(text):
    """Check if text is primarily Japanese using fastText"""
    if not text or len(text.strip()) == 0:
        return False

    config = get_config()
    lang_config = config["language_detection"]
    target_lang = lang_config["target_language"]
    confidence_threshold = lang_config["confidence_threshold"]

    model = get_language_model()
    prediction = model.predict(text.replace("\n", " "))
    label, confidence = prediction[0][0], prediction[1][0]

    # label format: '__label__ja'
    lang_code = label.replace("__label__", "")

    return lang_code == target_lang and confidence >= confidence_threshold

def has_any_labels(post):
    """Exclude posts that have any labels (moderation applied)"""
    labels = getattr(post, "labels", None)
    return bool(labels)

def extract_hashtag_count(record):
    """Extract hashtag count from record.facets"""
    if not record:
        return 0

    facets = getattr(record, "facets", None)
    if not facets:
        return 0

    hashtag_count = 0
    for facet in facets:
        # Facet has features list; check for hashtag type
        features = getattr(facet, "features", None) or []
        for feature in features:
            # Use py_type instead of $type (atproto uses py_type)
            py_type = getattr(feature, "py_type", None)
            # Hashtag facet type is "app.bsky.richtext.facet#tag"
            if py_type and "tag" in py_type:
                hashtag_count += 1

    return hashtag_count


def extract_hashtags(record):
    """Extract actual hashtag names from record.facets"""
    hashtags = []
    if not record:
        return hashtags

    facets = getattr(record, "facets", None)
    if not facets:
        return hashtags

    for facet in facets:
        features = getattr(facet, "features", None) or []
        for feature in features:
            # Use py_type instead of $type (atproto uses py_type)
            py_type = getattr(feature, "py_type", None)

            # Check if this is a hashtag facet
            if py_type and "tag" in py_type:
                tag = getattr(feature, "tag", None)
                if tag:
                    hashtags.append(tag)

    return hashtags


def search_posts_with_retry(client, search_config, max_retries=3):
    """
    Search posts with exponential backoff retry logic.

    Args:
        client: atproto Client instance
        search_config: Search configuration dict
        max_retries: Maximum number of retry attempts

    Returns:
        Search result or None if all retries failed
    """
    from atproto_client.exceptions import InvokeTimeoutError
    import httpx

    search_query = search_config["query"]
    search_limit = search_config["limit"]
    search_sort = search_config["sort"]

    for attempt in range(max_retries):
        try:
            print(f"Searching for posts: {search_query} (attempt {attempt + 1}/{max_retries})")
            res = client.app.bsky.feed.search_posts({
                "q": search_query,
                "sort": search_sort,
                "limit": search_limit,
            })
            print(f"Search successful on attempt {attempt + 1}")
            return res
        except (InvokeTimeoutError, httpx.TimeoutException) as e:
            if attempt < max_retries - 1:
                wait_time = 10 * (2 ** attempt)  # exponential backoff: 10s, 20s, 40s
                print(f"[RETRY] Search timed out. Retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                print(f"[ERROR] Search failed after {max_retries} attempts")
                raise


def lambda_handler(event, context):
    """
    Ingest Lambda: Fetches latest Japanese posts from Bluesky.

    Process:
    1. Search for posts with lang:ja (with retry on timeout)
    2. Filter out moderated posts (has_any_labels)
    3. Verify language with fastText
    4. Calculate density score
    5. Invoke Store Lambda asynchronously
    """
    try:
        from atproto import Client

        # Get credentials from Secrets Manager
        credentials = get_bsky_credentials()
        bsky_handle = credentials["handle"]
        bsky_app_password = credentials["appPassword"]

        # Authentication
        print("Authenticating with Bluesky...")
        client = Client()
        client.login(bsky_handle, bsky_app_password)

        # Search posts with retry logic
        config = get_config()
        search_config = config["search"]
        res = search_posts_with_retry(client, search_config, max_retries=3)

        # Process results
        items = []
        dense_texts = []  # Collect texts for Dense feed
        dense_base_forms = []  # Collect base forms (見出し語) for badword dictionary creation
        text_only_short_count = 0  # Count of text-only posts ≤15 chars
        skipped_by_reason = {
            "invalid_fields": 0,
            "moderation_labels": 0,
            "non_japanese": 0,
            "spam_hashtags": 0,
        }
        badword_stats = {
            "total_posts_with_badwords": 0,
            "total_badword_matches": 0,
            "badword_distribution": {},  # Count by number of matches per post
            "matched_words": {},  # Count by matched word name
        }

        posts = getattr(res, "posts", []) or []
        print(f"Found {len(posts)} posts")

        for post in posts:
            uri = post.uri
            indexed_at = post.indexed_at

            if not uri or not indexed_at:
                skipped_by_reason["invalid_fields"] += 1
                continue

            # Skip posts with labels (moderation)
            if has_any_labels(post):
                print(f"[FILTER] Moderation: {uri}")
                skipped_by_reason["moderation_labels"] += 1
                continue

            # Extract text and post attributes
            record = getattr(post, "record", None)
            text = getattr(record, "text", "") if record else ""

            # Verify Japanese with fastText
            if not is_japanese(text):
                print(f"[FILTER] Non-Japanese: {uri}")
                skipped_by_reason["non_japanese"] += 1
                continue

            # Extract post attributes
            is_reply = bool(getattr(record, "reply", None)) if record else False

            # Check for images and videos in embed
            has_images = False
            embed = getattr(record, "embed", None) if record else None
            if embed:
                # Check for images (including GIFs) or videos
                images = getattr(embed, "images", None)
                video = getattr(embed, "video", None)
                has_images = bool(images or video)

            # Extract hashtag count and names from facets
            hashtag_count = extract_hashtag_count(record)
            hashtags = extract_hashtags(record)

            # Skip posts with 5+ hashtags (spam detection)
            # Rationale: Posts with 5+ hashtags are considered spam/low-quality
            # - Excessive hashtags indicate artificial reach-seeking behavior
            # - Natural Japanese posts typically use 1-3 hashtags
            # - 5+ hashtags = search optimization + low semantic value
            # - Incompatible with "calm and safe" feed goal
            if hashtag_count >= 5:
                print(f"[FILTER] Spam (hashtags >= 5): {uri} ({hashtag_count} hashtags)")
                skipped_by_reason["spam_hashtags"] += 1
                continue

            # Calculate density score with attributes
            density_score, badword_count, matched_words = calculate_density_score(text, is_reply=is_reply, has_images=has_images, hashtag_count=hashtag_count)

            # Record badword statistics
            if badword_count > 0:
                badword_stats["total_posts_with_badwords"] += 1
                badword_stats["total_badword_matches"] += badword_count
                # Track distribution of matches per post
                key = str(badword_count)
                badword_stats["badword_distribution"][key] = badword_stats["badword_distribution"].get(key, 0) + 1
                # Track which words were matched
                for word in matched_words:
                    badword_stats["matched_words"][word] = badword_stats["matched_words"].get(word, 0) + 1

            # Count text-only short posts (density_score = 0.0)
            if density_score == 0.0:
                text_only_short_count += 1

            # Convert indexed_at to timestamp
            ts = time.mktime(time.strptime(indexed_at, "%Y-%m-%dT%H:%M:%S.%fZ"))

            items.append({
                "uri": uri,
                "ts": ts,
                "density_score": density_score,
                "hashtags": hashtags,
            })

            # Collect text and base forms if it will go to Dense feed
            if density_score >= get_density_threshold():
                # Escape newlines for single-line format
                text_escaped = text.replace("\n", "\\n").replace("\r", "\\r")
                dense_texts.append(text_escaped)

                # Extract base forms (見出し語) for badword dictionary creation
                try:
                    base_forms = extract_base_forms(text)
                    dense_base_forms.extend(base_forms)
                except Exception as e:
                    print(f"[EXTRACT_BASE_FORMS_ERROR] {uri}: {e}")

            print(f"[ADDED] {uri} (density={density_score:.3f})")

        print(f"\n=== Processing Summary ===")
        print(f"Total fetched: {len(posts)}")
        print(f"  - Invalid fields: {skipped_by_reason['invalid_fields']}")
        print(f"  - Moderation labels: {skipped_by_reason['moderation_labels']}")
        print(f"  - Non-Japanese: {skipped_by_reason['non_japanese']}")
        print(f"  - Spam hashtags (5+): {skipped_by_reason['spam_hashtags']}")
        print(f"  - Passed filters: {len(items)}")

        # Badword statistics
        print(f"\n=== Badword Analysis ===")
        badword_hit_rate = (badword_stats['total_posts_with_badwords'] / len(items) * 100) if items else 0
        print(f"Posts with badwords: {badword_stats['total_posts_with_badwords']} / {len(items)} ({badword_hit_rate:.1f}%)")
        print(f"Total badword matches: {badword_stats['total_badword_matches']}")
        if badword_stats['total_posts_with_badwords'] > 0:
            avg_matches_per_hit = badword_stats['total_badword_matches'] / badword_stats['total_posts_with_badwords']
            print(f"Average matches per badword-hit post: {avg_matches_per_hit:.2f}")
        if badword_stats['badword_distribution']:
            print(f"Distribution by matches per post:")
            for match_count in sorted(badword_stats['badword_distribution'].keys(), key=int):
                count = badword_stats['badword_distribution'][match_count]
                print(f"  - {match_count} match(es): {count} post(s)")

        print(f"\n=== Dense Feed Statistics ===")
        dense_rate = (len(dense_texts) / len(items) * 100) if items else 0
        print(f"Dense posts: {len(dense_texts)} / {len(items)} ({dense_rate:.1f}%)")
        print(f"=========================\n")

        # Calculate total skipped
        total_skipped = sum(skipped_by_reason.values())

        # Prepare statistics data for StatsLambda
        now_jst = datetime.now(JST)
        timestamp = now_jst.strftime("%Y%m%d_%H%M%S")
        iso_timestamp = now_jst.strftime("%Y-%m-%d %H:%M:%S")

        # Calculate rates
        total_fetched = len(posts)
        passed_filters = len(items)
        dense_rate = (len(dense_texts) / len(items) * 100) if items else 0

        # Badword metrics
        badword_hit_rate = (badword_stats['total_posts_with_badwords'] / passed_filters * 100) if passed_filters else 0
        avg_matches = (badword_stats['total_badword_matches'] / badword_stats['total_posts_with_badwords']
                      if badword_stats['total_posts_with_badwords'] > 0 else 0)

        # Sort matched words by count (descending)
        matched_words_sorted = [
            {"word": word, "count": count}
            for word, count in sorted(
                badword_stats['matched_words'].items(),
                key=lambda x: x[1],
                reverse=True
            )
        ]

        # Build statistics payload for StatsLambda
        stats_payload = {
            "execution_time": iso_timestamp,
            "timestamp": timestamp,
            "processing_summary": {
                "total_fetched": total_fetched,
                "invalid_fields": skipped_by_reason['invalid_fields'],
                "moderation_labels": skipped_by_reason['moderation_labels'],
                "non_japanese": skipped_by_reason['non_japanese'],
                "spam_hashtags": skipped_by_reason['spam_hashtags'],
                "passed_filters": passed_filters,
                "rates": {
                    "invalid_fields_rate": round(skipped_by_reason['invalid_fields'] / total_fetched * 100, 1) if total_fetched else 0,
                    "moderation_labels_rate": round(skipped_by_reason['moderation_labels'] / total_fetched * 100, 1) if total_fetched else 0,
                    "non_japanese_rate": round(skipped_by_reason['non_japanese'] / total_fetched * 100, 1) if total_fetched else 0,
                    "spam_hashtags_rate": round(skipped_by_reason['spam_hashtags'] / total_fetched * 100, 1) if total_fetched else 0,
                    "passed_filters_rate": round(passed_filters / total_fetched * 100, 1) if total_fetched else 0,
                }
            },
            "badword_analysis": {
                "posts_with_badwords": badword_stats['total_posts_with_badwords'],
                "hit_rate": round(badword_hit_rate, 1),
                "total_matches": badword_stats['total_badword_matches'],
                "avg_matches_per_hit": round(avg_matches, 2),
                "matched_words": matched_words_sorted,
                "distribution": badword_stats['badword_distribution']
            },
            "dense_feed": {
                "total_items": passed_filters,
                "text_only_short": text_only_short_count,
                "dense_posts": len(dense_texts),
                "dense_rate": round(dense_rate, 1)
            },
            "version": "1.0"
        }

        # Check previous day's daily file and query CloudWatch if needed
        now_jst = datetime.now(JST)
        yesterday = now_jst - timedelta(days=1)
        yesterday_date = yesterday.strftime("%Y-%m-%d")

        getfeed_stats = {"total_invocations": 0}

        # Check if yesterday's daily file exists in STATISTICS_BUCKET
        try:
            s3_client = boto3.client("s3")
            statistics_bucket = os.environ.get("STATISTICS_BUCKET", "")
            daily_key = f"stats/daily/stats-{yesterday_date}.json"
            s3_client.head_object(Bucket=statistics_bucket, Key=daily_key)
        except ClientError as e:
            # Check if it's a 404 (NoSuchKey)
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                # Daily file doesn't exist, query CloudWatch
                getfeed_invocations = get_getfeed_invocations_for_date(yesterday_date)
                getfeed_stats["total_invocations"] = getfeed_invocations
        except Exception as e:
            pass

        # Invoke Store Lambda asynchronously
        if items:
            lambda_client = boto3.client("lambda")
            payload = {
                "items": items,
                "batch_stats": stats_payload,
                "dense_texts": dense_texts,
                "dense_base_forms": dense_base_forms,
                "getfeed_stats": getfeed_stats
            }

            response = lambda_client.invoke(
                FunctionName=STORE_FUNCTION_NAME,
                InvocationType="Event",  # Asynchronous
                Payload=json.dumps(payload),
            )

        return {
            "fetched": len(items),
            "skipped": total_skipped,
            "dense_posts": len(dense_texts),
            "total_base_forms": len(dense_base_forms),
            "getfeed_stats": getfeed_stats
        }

    except Exception as e:
        print(f"Ingest Lambda error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "error": str(e),
            "fetched": 0,
            "skipped": 0,
        }
