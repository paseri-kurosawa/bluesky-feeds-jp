import os
import json
import time
import boto3
import unicodedata
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta
from density_scorer import calculate_density_score, tokenize_japanese, extract_base_forms, load_badwords_config

# Japan Standard Time (UTC+9)
JST = timezone(timedelta(hours=9))

# AWS Clients
cloudwatch_client = boto3.client("cloudwatch")
logs_client = boto3.client("logs")
s3_client = boto3.client("s3")

# CloudWatch Logs query function for feed-specific calls
def get_getfeed_calls_by_feed_type(target_date):
    """
    Query GetFeed Lambda logs from CloudWatch Logs to count calls by feed type.

    Args:
        target_date: Date string in format YYYY-MM-DD (JST)

    Returns:
        Tuple of (getfeed_stats_raw_dense, getfeed_stats_stablehashtag)
        - getfeed_stats_raw_dense: Dict with raw_calls, dense_calls, total_invocations (raw + dense)
        - getfeed_stats_stablehashtag: Dict with stablehashtag_calls, total_invocations
    """
    try:
        # Parse target date as JST
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")

        # Create JST-aware datetime for the entire day (00:00 - 23:59:59 JST)
        start_time_jst = date_obj.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=JST)
        end_time_jst = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=JST)

        # Convert JST to Unix timestamp (UTC)
        start_time_s = int(start_time_jst.timestamp())
        end_time_s = int(end_time_jst.timestamp())

        # GetFeed Lambda log group from config
        config = load_config()
        log_group = config.get("aws_lambda", {}).get("getfeed_log_group", "")

        print(f"[LOGS] Querying GetFeed Lambda logs for {target_date}")

        # Query for each feed type from [FEED_ACCESS] logs
        query_patterns = {
            'raw_calls': 'feed_type=raw',
            'dense_calls': 'feed_type=dense',
            'stablehashtag_calls': 'feed_type=stablehashtag'
        }

        results = {}

        for field_name, pattern in query_patterns.items():
            try:
                query_string = f'fields @timestamp | filter @message like /\\[FEED_ACCESS\\].*{pattern}/ | stats count() as count'
                response = logs_client.start_query(
                    logGroupName=log_group,
                    startTime=start_time_s,
                    endTime=end_time_s,
                    queryString=query_string
                )
                query_id = response['queryId']

                # Wait for query to complete
                for _ in range(30):  # Max 30 seconds
                    result = logs_client.get_query_results(queryId=query_id)
                    if result['status'] == 'Complete':
                        count = 0
                        if result['results']:
                            for record in result['results']:
                                for field in record:
                                    if field['field'] == 'count':
                                        count = int(field['value'])
                        results[field_name] = count
                        break
                    time.sleep(1)
            except Exception as e:
                print(f"[LOGS] Error querying {field_name}: {str(e)}")
                results[field_name] = 0

        raw_calls = results.get('raw_calls', 0)
        dense_calls = results.get('dense_calls', 0)
        stablehashtag_calls = results.get('stablehashtag_calls', 0)

        # Build separate getfeed_stats for raw-dense and stablehashtag
        getfeed_stats_raw_dense = {
            'raw_calls': raw_calls,
            'dense_calls': dense_calls,
            'total_invocations': raw_calls + dense_calls
        }

        getfeed_stats_stablehashtag = {
            'stablehashtag_calls': stablehashtag_calls,
            'total_invocations': stablehashtag_calls
        }

        print(f"[LOGS] Feed calls for {target_date}: raw={raw_calls}, dense={dense_calls}, stablehashtag={stablehashtag_calls}")
        return getfeed_stats_raw_dense, getfeed_stats_stablehashtag

    except Exception as e:
        print(f"[LOGS] Error querying feed calls for {target_date}: {str(e)}")
        import traceback
        traceback.print_exc()
        return (
            {'raw_calls': 0, 'dense_calls': 0, 'total_invocations': 0},
            {'stablehashtag_calls': 0, 'total_invocations': 0}
        )

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
GETFEED_LAMBDA_NAME = os.environ.get("GETFEED_LAMBDA_NAME", "")

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


def search_posts_with_retry(client, search_query, search_config, max_retries=3):
    """
    Search posts with exponential backoff retry logic.

    Args:
        client: atproto Client instance
        search_query: Query string to search for
        search_config: Search configuration dict (contains limit and sort)
        max_retries: Maximum number of retry attempts

    Returns:
        Search result or None if all retries failed
    """
    from atproto_client.exceptions import InvokeTimeoutError
    import httpx

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


def load_stable_ranking(bucket):
    """Load stable hashtag ranking from hashtags/datasource/stable_ranking.json"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key="hashtags/datasource/stable_ranking.json")
        data = json.loads(response["Body"].read().decode("utf-8"))
        tags = data.get("top_hashtags", [])
        print(f"[DATASOURCE] Loaded {len(tags)} stable hashtags from datasource/stable_ranking.json")
        return tags
    except Exception as e:
        print(f"[DATASOURCE] Error loading stable_ranking.json: {e}")
        return []


def load_latest_batch(bucket):
    """Load latest batch hashtags from hashtags/batch/ directory"""
    try:
        # List all batch files and find the latest one
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix="hashtags/batch/")
        if "Contents" not in response:
            print("[DATASOURCE] No batch files found in hashtags/batch/")
            return {}

        # Sort by LastModified, get the latest
        files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
        if not files:
            print("[DATASOURCE] No batch files found")
            return {}

        latest_key = files[0]["Key"]
        print(f"[DATASOURCE] Loading latest batch: {latest_key}")

        # Get the latest batch file
        batch_response = s3_client.get_object(Bucket=bucket, Key=latest_key)
        batch_data = json.loads(batch_response["Body"].read().decode("utf-8"))

        # batch_data is {tag: count, ...}
        # Convert tags to lowercase for consistency
        batch_dict = {tag.lower(): count for tag, count in batch_data.items()}
        print(f"[DATASOURCE] Loaded {len(batch_dict)} unique hashtags from latest batch: {latest_key}")
        return batch_dict
    except Exception as e:
        print(f"[DATASOURCE] Error loading latest batch: {e}")
        return {}



def get_and_select_hot_hashtag(bucket):
    """
    Get intersection of latest batch and stable hashtags, then select one.

    Args:
        bucket: S3 bucket name

    Returns:
        Selected hashtag name (lowercase), or None if no hot detected
    """
    # Load both datasources
    stable_hashtags = load_stable_ranking(bucket)
    latest_batch = load_latest_batch(bucket)

    if not stable_hashtags or not latest_batch:
        print("[HOT-DRIVEN] No stable or batch hashtags available")
        return None

    # Convert stable list to set of lowercase tag names
    stable_tags_set = {tag_dict["tag"].lower() for tag_dict in stable_hashtags}
    batch_tags_set = set(latest_batch.keys())

    # Get intersection
    intersection = stable_tags_set & batch_tags_set

    if not intersection:
        print("[HOT-DRIVEN] No intersection between batch and stable hashtags")
        return None

    print(f"[HOT-DRIVEN] Found {len(intersection)} batch+stable hashtags: {intersection}")

    # Select 1 hashtag from intersection
    selected_hot_tag = select_hot_hashtag(list(intersection), latest_batch, stable_hashtags)
    return selected_hot_tag


def select_hot_hashtag(hot_and_stable, latest_batch, stable_hashtags_list):
    """
    Select 1 hashtag from multiple hot+stable candidates.

    Priority 1: Most appearances (hot degree)
    Priority 2: Lowest position in stable list (rarity - for diversity)

    Args:
        hot_and_stable: List of tag names (lowercase)
        latest_batch: Dict {tag: count}
        stable_hashtags_list: List of dicts [{tag: ..., count: ...}]

    Returns:
        Selected hashtag name (lowercase)
    """
    if not hot_and_stable:
        return None

    if len(hot_and_stable) == 1:
        return hot_and_stable[0]

    # Build stable position map for tie-breaking
    stable_position_map = {
        tag_dict["tag"].lower(): idx
        for idx, tag_dict in enumerate(stable_hashtags_list)
    }

    # Sort by: (1) appearance count desc, (2) stable position asc
    selected = sorted(
        hot_and_stable,
        key=lambda tag: (
            -latest_batch.get(tag, 0),  # Appearance count (descending)
            stable_position_map.get(tag, float('inf'))  # Position in stable list (ascending)
        )
    )[0]

    print(f"[HOT-DRIVEN] Selected: {selected} (count={latest_batch.get(selected, 0)}, stable_pos={stable_position_map.get(selected, 'N/A')})")
    return selected


def has_hashtags(item):
    """Check if item has any hashtags"""
    hashtags = item.get("hashtags", [])
    return len(hashtags) > 0


def extract_hashtag_posts(items):
    """
    Extract only posts with hashtags from a list of items.
    Preserves order.

    Args:
        items: List of post items

    Returns:
        List of items that have hashtags
    """
    tagged_items = [item for item in items if has_hashtags(item)]
    print(f"[LAYER2] Extracted {len(tagged_items)} posts with hashtags from {len(items)} total")
    return tagged_items



def process_posts_with_filters(posts, feed_type="raw"):
    """
    Process posts with common filters and scoring.

    Args:
        posts: List of posts from Bluesky search
        feed_type: "raw" or "stablehashtag" (for logging)

    Returns:
        Tuple: (items, dense_texts, dense_base_forms, badword_stats, skipped_by_reason)
    """
    items = []
    dense_texts = []
    dense_base_forms = []
    skipped_by_reason = {
        "invalid_fields": 0,
        "moderation_labels": 0,
        "non_japanese": 0,
        "spam_hashtags": 0,
    }
    badword_stats = {
        "total_posts_with_badwords": 0,
        "total_badword_matches": 0,
        "badword_distribution": {},
        "matched_words": {},
    }

    print(f"\n[{feed_type.upper()}] Processing {len(posts)} posts with filters...")

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
            images = getattr(embed, "images", None)
            video = getattr(embed, "video", None)
            has_images = bool(images or video)

        # Extract hashtag count and names from facets
        hashtag_count = extract_hashtag_count(record)
        hashtags = extract_hashtags(record)

        # Skip posts with 5+ hashtags (spam detection)
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
            key = str(badword_count)
            badword_stats["badword_distribution"][key] = badword_stats["badword_distribution"].get(key, 0) + 1
            for word in matched_words:
                badword_stats["matched_words"][word] = badword_stats["matched_words"].get(word, 0) + 1

        # Convert indexed_at to timestamp
        ts = time.mktime(time.strptime(indexed_at, "%Y-%m-%dT%H:%M:%S.%fZ"))

        # Normalize hashtags (Unicode NFC + lowercase)
        normalized_hashtags = [unicodedata.normalize("NFC", tag).lower() for tag in hashtags]

        items.append({
            "uri": uri,
            "ts": ts,
            "density_score": density_score,
            "hashtags": normalized_hashtags,
        })

        # Collect text and base forms if it will go to Dense feed
        if density_score >= get_density_threshold():
            text_escaped = text.replace("\n", "\\n").replace("\r", "\\r")
            dense_texts.append(text_escaped)

            try:
                base_forms = extract_base_forms(text)
                dense_base_forms.extend(base_forms)
            except Exception as e:
                print(f"[EXTRACT_BASE_FORMS_ERROR] {uri}: {e}")

        print(f"[ADDED] {uri} (density={density_score:.3f})")

    return items, dense_texts, dense_base_forms, badword_stats, skipped_by_reason


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

        # Initialize separated getfeed_stats for raw-dense and stablehashtag (will be updated later)
        getfeed_stats_raw_dense = {
            "raw_calls": 0,
            "dense_calls": 0,
            "total_invocations": 0
        }
        getfeed_stats_stablehashtag = {
            "stablehashtag_calls": 0,
            "total_invocations": 0
        }

        # Get credentials from Secrets Manager
        credentials = get_bsky_credentials()
        bsky_handle = credentials["handle"]
        bsky_app_password = credentials["appPassword"]

        # Authentication
        print("Authenticating with Bluesky...")
        client = Client()
        client.login(bsky_handle, bsky_app_password)

        # Get statistics bucket
        statistics_bucket = os.environ.get("STATISTICS_BUCKET", "")

        # === QUERY 1: lang:ja (RAW/DENSE) ===
        config = get_config()
        search_config = config["search"]
        search_query_1 = search_config["query"]  # Should be "lang:ja"

        res_1 = search_posts_with_retry(client, search_query_1, search_config, max_retries=3)
        posts_1 = getattr(res_1, "posts", []) or []
        print(f"[QUERY1] Found {len(posts_1)} posts for: {search_query_1}")

        # Process Query 1 posts
        items_raw, dense_texts, dense_base_forms, badword_stats, skipped_by_reason = process_posts_with_filters(posts_1, feed_type="raw")

        # Count text-only short posts
        text_only_short_count = sum(1 for item in items_raw if item["density_score"] == 0.0)

        # === Extract and count hashtags from THIS BATCH (Raw feed) ===
        # Normalize tags (Unicode NFC + lowercase) to absorb variation
        hashtag_counts = {}
        for item in items_raw:
            hashtags = item.get("hashtags", [])
            for tag in hashtags:
                normalized_tag = unicodedata.normalize("NFC", tag).lower()
                hashtag_counts[normalized_tag] = hashtag_counts.get(normalized_tag, 0) + 1

        # Filter hashtags by badwords (remove inappropriate tags)
        # Partial matching: exclude hashtag if it contains any badword
        try:
            badwords_config = load_badwords_config()
            badwords_set = {word.lower() for word in badwords_config.get("badwords", [])}

            filtered_hashtag_counts = {}
            filtered_out_count = 0

            for tag, count in hashtag_counts.items():
                tag_lower = tag.lower()
                # Check if any badword is contained in the hashtag
                has_badword = any(badword in tag_lower for badword in badwords_set)
                if not has_badword:
                    filtered_hashtag_counts[tag] = count
                else:
                    filtered_out_count += 1

            hashtag_counts = filtered_hashtag_counts

        except Exception as e:
            print(f"[HASHTAG FILTER ERROR] Failed to filter hashtags: {str(e)}")
            import traceback
            traceback.print_exc()

        # === Select hot hashtag from THIS BATCH's hashtags ===
        selected_hot_tag = None
        selection_method = None
        try:
            if hashtag_counts:
                # Load stable ranking for intersection check
                config = load_config()
                s3_key = config.get("s3_keys", {}).get("stable_hashtags_from_raw_posts", "components/stable_hashtags_from_raw_posts.json")

                try:
                    s3 = boto3.client("s3")
                    response = s3.get_object(Bucket=statistics_bucket, Key=s3_key)
                    stable_data = json.loads(response["Body"].read().decode("utf-8"))
                    stable_hashtags = stable_data.get("top_hashtags", [])
                except Exception as e:
                    print(f"[HOT-DRIVEN] Failed to load stable hashtags from {s3_key}: {e}")
                    import traceback
                    traceback.print_exc()
                    stable_hashtags = []

                stable_tags_set = {tag_dict["tag"].lower() for tag_dict in stable_hashtags}
                batch_tags_set = set(hashtag_counts.keys())

                # Get intersection
                intersection = stable_tags_set & batch_tags_set

                if intersection:
                    print(f"[HOT-DRIVEN] Found {len(intersection)} batch+stable hashtags: {intersection}")
                    # Select 1 hashtag from intersection
                    selected_hot_tag = select_hot_hashtag(list(intersection), hashtag_counts, stable_hashtags)
                    selection_method = "batch_stable_intersection"
                else:
                    print("[HOT-DRIVEN] No intersection between batch and stable hashtags")
                    selection_method = "dense_fallback"
            else:
                print("[HOT-DRIVEN] No hashtags in this batch")
                selection_method = "dense_fallback"
        except Exception as e:
            print(f"[HOT-DRIVEN] Error selecting hot hashtag: {e}")
            import traceback
            traceback.print_exc()
            selection_method = "dense_fallback"

        # === QUERY 2: lang:ja #<hot_hashtag> (STABLETAG - HOT DRIVEN) ===
        items_stablehashtag = []
        stablehashtag_posts_count = 0
        stats_payload_stablehashtag = None
        dense_texts_stablehashtag = []
        dense_base_forms_stablehashtag = []
        badword_stats_stablehashtag = None
        skipped_by_reason_stablehashtag = None

        try:
            if selected_hot_tag:
                search_query_2 = f"lang:ja #{selected_hot_tag}"
                print(f"[HOT-DRIVEN] Querying with selected hot hashtag: {search_query_2}")

                res_2 = search_posts_with_retry(client, search_query_2, search_config, max_retries=3)
                posts_2 = getattr(res_2, "posts", []) or []
                print(f"[QUERY2] Found {len(posts_2)} posts for: {search_query_2}")

                items_stablehashtag, dense_texts_stablehashtag, dense_base_forms_stablehashtag, badword_stats_stablehashtag, skipped_by_reason_stablehashtag = process_posts_with_filters(posts_2, feed_type="stablehashtag")
                stablehashtag_posts_count = len(posts_2)
            else:
                # Layer 1 no hot detected - Layer 2 fallback from Dense feed
                print("[HOT-DRIVEN] No hot+stable hashtags detected. Using Layer 2 (Dense fallback)")

                # Extract hashtag-bearing posts from Dense feed (items_raw with density_score >= threshold)
                dense_posts = [item for item in items_raw if item.get("density_score", 0) >= get_density_threshold()]
                tagged_dense_posts = extract_hashtag_posts(dense_posts)

                if tagged_dense_posts:
                    # Use tagged Dense posts for stablehashtag feed
                    items_stablehashtag = tagged_dense_posts
                    stablehashtag_posts_count = len(items_raw)  # Original raw count for stats

                    # Layer 2 fallback: No statistics (fallback mode)
                    badword_stats_stablehashtag = {
                        "total_posts_with_badwords": 0,
                        "total_badword_matches": 0,
                        "badword_distribution": {},
                        "matched_words": {},
                    }
                    skipped_by_reason_stablehashtag = {
                        "invalid_fields": 0,
                        "moderation_labels": 0,
                        "non_japanese": 0,
                        "spam_hashtags": 0,
                    }
                    dense_texts_stablehashtag = []  # No dense texts for fallback
                    dense_base_forms_stablehashtag = []  # No base forms for fallback

                    print(f"[LAYER2] Using {len(items_stablehashtag)} tagged Dense posts for stablehashtag feed (statistics disabled for fallback)")
                else:
                    # No tagged Dense posts available
                    print("[LAYER2] No tagged posts in Dense feed")
                    items_stablehashtag = []
                    badword_stats_stablehashtag = {
                        "total_posts_with_badwords": 0,
                        "total_badword_matches": 0,
                        "badword_distribution": {},
                        "matched_words": {},
                    }
                    skipped_by_reason_stablehashtag = {
                        "invalid_fields": 0,
                        "moderation_labels": 0,
                        "non_japanese": 0,
                        "spam_hashtags": 0,
                    }
        except Exception as e:
            print(f"[QUERY2] Error in hot-driven logic: {e}")
            import traceback
            traceback.print_exc()
            badword_stats_stablehashtag = {
                "total_posts_with_badwords": 0,
                "total_badword_matches": 0,
                "badword_distribution": {},
                "matched_words": {},
            }
            skipped_by_reason_stablehashtag = {
                "invalid_fields": 0,
                "moderation_labels": 0,
                "non_japanese": 0,
                "spam_hashtags": 0,
            }

        print(f"\n=== Processing Summary ===")
        print(f"Raw posts: {len(posts_1)}")
        print(f"  - Invalid fields: {skipped_by_reason['invalid_fields']}")
        print(f"  - Moderation labels: {skipped_by_reason['moderation_labels']}")
        print(f"  - Non-Japanese: {skipped_by_reason['non_japanese']}")
        print(f"  - Spam hashtags (5+): {skipped_by_reason['spam_hashtags']}")
        print(f"  - Passed filters: {len(items_raw)}")

        print(f"\nStablehashtag posts: {stablehashtag_posts_count}")
        print(f"  - Passed filters: {len(items_stablehashtag)}")

        print(f"\n=== Badword Analysis (RAW) ===")
        badword_hit_rate = (badword_stats['total_posts_with_badwords'] / len(items_raw) * 100) if items_raw else 0
        print(f"Posts with badwords: {badword_stats['total_posts_with_badwords']} / {len(items_raw)} ({badword_hit_rate:.1f}%)")
        print(f"Total badword matches: {badword_stats['total_badword_matches']}")
        if badword_stats['total_posts_with_badwords'] > 0:
            avg_matches_per_hit = badword_stats['total_badword_matches'] / badword_stats['total_posts_with_badwords']
            print(f"Average matches per badword-hit post: {avg_matches_per_hit:.2f}")
        if badword_stats['badword_distribution']:
            print(f"Distribution by matches per post:")
            for match_count in sorted(badword_stats['badword_distribution'].keys(), key=int):
                count = badword_stats['badword_distribution'][match_count]
                print(f"  - {match_count} match(es): {count} post(s)")

        print(f"\n=== Dense Feed Statistics (RAW) ===")
        dense_rate = (len(dense_texts) / len(items_raw) * 100) if items_raw else 0
        print(f"Dense posts: {len(dense_texts)} / {len(items_raw)} ({dense_rate:.1f}%)")
        print(f"=========================\n")

        # Calculate total skipped
        total_skipped = sum(skipped_by_reason.values())

        # Check previous day's daily file and query CloudWatch Logs BEFORE building stats_payload
        now_jst = datetime.now(JST)
        yesterday = now_jst - timedelta(days=1)
        yesterday_date = yesterday.strftime("%Y-%m-%d")

        # Check if yesterday's daily file exists in STATISTICS_BUCKET (new format: raw-dense and stablehashtag)
        try:
            s3_client = boto3.client("s3")
            statistics_bucket = os.environ.get("STATISTICS_BUCKET", "")
            daily_key_raw_dense = f"stats/daily/raw-dense/stats-{yesterday_date}.json"
            daily_key_stablehashtag = f"stats/daily/stablehashtag/stats-{yesterday_date}.json"

            # Check if both files exist
            raw_dense_exists = False
            stablehashtag_exists = False

            try:
                s3_client.head_object(Bucket=statistics_bucket, Key=daily_key_raw_dense)
                raw_dense_exists = True
            except:
                pass

            try:
                s3_client.head_object(Bucket=statistics_bucket, Key=daily_key_stablehashtag)
                stablehashtag_exists = True
            except:
                pass

            # If either daily file is missing, query CloudWatch Logs for feed-specific calls
            if not raw_dense_exists or not stablehashtag_exists:
                getfeed_stats_raw_dense, getfeed_stats_stablehashtag = get_getfeed_calls_by_feed_type(yesterday_date)
        except Exception as e:
            print(f"[WARN] Failed to check daily files or query CloudWatch: {str(e)}")

        # Prepare statistics data for StatsLambda
        timestamp = now_jst.strftime("%Y%m%d_%H%M%S")
        iso_timestamp = now_jst.strftime("%Y-%m-%d %H:%M:%S")

        # === QUERY 1: raw-dense フィード統計 ===
        total_fetched_raw = len(posts_1)
        passed_filters_raw = len(items_raw)
        dense_rate_raw = (len(dense_texts) / len(items_raw) * 100) if items_raw else 0
        text_only_short_count_raw = sum(1 for item in items_raw if item["density_score"] == 0.0)

        badword_hit_rate_raw = (badword_stats['total_posts_with_badwords'] / passed_filters_raw * 100) if passed_filters_raw else 0
        avg_matches_raw = (badword_stats['total_badword_matches'] / badword_stats['total_posts_with_badwords']
                          if badword_stats['total_posts_with_badwords'] > 0 else 0)

        matched_words_sorted_raw = [
            {"word": word, "count": count}
            for word, count in sorted(
                badword_stats['matched_words'].items(),
                key=lambda x: x[1],
                reverse=True
            )
        ]

        stats_payload_raw = {
            "execution_time": iso_timestamp,
            "timestamp": timestamp,
            "processing_summary": {
                "total_fetched": total_fetched_raw,
                "invalid_fields": skipped_by_reason['invalid_fields'],
                "moderation_labels": skipped_by_reason['moderation_labels'],
                "non_japanese": skipped_by_reason['non_japanese'],
                "spam_hashtags": skipped_by_reason['spam_hashtags'],
                "passed_filters": passed_filters_raw,
                "rates": {
                    "invalid_fields_rate": round(skipped_by_reason['invalid_fields'] / total_fetched_raw * 100, 1) if total_fetched_raw else 0,
                    "moderation_labels_rate": round(skipped_by_reason['moderation_labels'] / total_fetched_raw * 100, 1) if total_fetched_raw else 0,
                    "non_japanese_rate": round(skipped_by_reason['non_japanese'] / total_fetched_raw * 100, 1) if total_fetched_raw else 0,
                    "spam_hashtags_rate": round(skipped_by_reason['spam_hashtags'] / total_fetched_raw * 100, 1) if total_fetched_raw else 0,
                    "passed_filters_rate": round(passed_filters_raw / total_fetched_raw * 100, 1) if total_fetched_raw else 0,
                }
            },
            "badword_analysis": {
                "posts_with_badwords": badword_stats['total_posts_with_badwords'],
                "hit_rate": round(badword_hit_rate_raw, 1),
                "total_matches": badword_stats['total_badword_matches'],
                "avg_matches_per_hit": round(avg_matches_raw, 2),
                "matched_words": matched_words_sorted_raw,
                "distribution": badword_stats['badword_distribution']
            },
            "dense_feed": {
                "total_items": passed_filters_raw,
                "text_only_short": text_only_short_count_raw,
                "dense_posts": len(dense_texts),
                "dense_rate": round(dense_rate_raw, 1)
            },
            "getfeed_stats": getfeed_stats_raw_dense,
            "version": "1.0"
        }

        # === QUERY 2: stablehashtag フィード統計 ===
        total_fetched_stablehashtag = stablehashtag_posts_count
        passed_filters_stablehashtag = len(items_stablehashtag)
        dense_rate_stablehashtag = (len(dense_texts_stablehashtag) / len(items_stablehashtag) * 100) if items_stablehashtag else 0
        text_only_short_count_stablehashtag = sum(1 for item in items_stablehashtag if item["density_score"] == 0.0)

        badword_hit_rate_stablehashtag = (badword_stats_stablehashtag['total_posts_with_badwords'] / passed_filters_stablehashtag * 100) if passed_filters_stablehashtag else 0
        avg_matches_stablehashtag = (badword_stats_stablehashtag['total_badword_matches'] / badword_stats_stablehashtag['total_posts_with_badwords']
                                    if badword_stats_stablehashtag['total_posts_with_badwords'] > 0 else 0)

        matched_words_sorted_stablehashtag = [
            {"word": word, "count": count}
            for word, count in sorted(
                badword_stats_stablehashtag['matched_words'].items(),
                key=lambda x: x[1],
                reverse=True
            )
        ]

        stats_payload_stablehashtag = {
            "execution_time": iso_timestamp,
            "timestamp": timestamp,
            "processing_summary": {
                "total_fetched": total_fetched_stablehashtag,
                "invalid_fields": skipped_by_reason_stablehashtag['invalid_fields'],
                "moderation_labels": skipped_by_reason_stablehashtag['moderation_labels'],
                "non_japanese": skipped_by_reason_stablehashtag['non_japanese'],
                "spam_hashtags": skipped_by_reason_stablehashtag['spam_hashtags'],
                "passed_filters": passed_filters_stablehashtag,
                "rates": {
                    "invalid_fields_rate": round(skipped_by_reason_stablehashtag['invalid_fields'] / total_fetched_stablehashtag * 100, 1) if total_fetched_stablehashtag else 0,
                    "moderation_labels_rate": round(skipped_by_reason_stablehashtag['moderation_labels'] / total_fetched_stablehashtag * 100, 1) if total_fetched_stablehashtag else 0,
                    "non_japanese_rate": round(skipped_by_reason_stablehashtag['non_japanese'] / total_fetched_stablehashtag * 100, 1) if total_fetched_stablehashtag else 0,
                    "spam_hashtags_rate": round(skipped_by_reason_stablehashtag['spam_hashtags'] / total_fetched_stablehashtag * 100, 1) if total_fetched_stablehashtag else 0,
                    "passed_filters_rate": round(passed_filters_stablehashtag / total_fetched_stablehashtag * 100, 1) if total_fetched_stablehashtag else 0,
                }
            },
            "badword_analysis": {
                "posts_with_badwords": badword_stats_stablehashtag['total_posts_with_badwords'],
                "hit_rate": round(badword_hit_rate_stablehashtag, 1),
                "total_matches": badword_stats_stablehashtag['total_badword_matches'],
                "avg_matches_per_hit": round(avg_matches_stablehashtag, 2),
                "matched_words": matched_words_sorted_stablehashtag,
                "distribution": badword_stats_stablehashtag['badword_distribution']
            },
            "dense_feed": {
                "total_items": passed_filters_stablehashtag,
                "text_only_short": text_only_short_count_stablehashtag,
                "dense_posts": len(dense_texts_stablehashtag),
                "dense_rate": round(dense_rate_stablehashtag, 1)
            },
            "getfeed_stats": getfeed_stats_stablehashtag,
            "version": "1.0"
        }

        # Invoke Store Lambda asynchronously
        if items_raw or items_stablehashtag:
            lambda_client = boto3.client("lambda")
            payload = {
                "items_raw": items_raw,
                "items_stablehashtag": items_stablehashtag,
                "batch_stats_raw": stats_payload_raw,
                "batch_stats_stablehashtag": stats_payload_stablehashtag,
                "dense_texts": dense_texts,
                "dense_base_forms": dense_base_forms,
                "hashtags": hashtag_counts,
                "selected_hot_tag": selected_hot_tag,
                "selection_method": selection_method
            }

            try:
                response = lambda_client.invoke(
                    FunctionName=STORE_FUNCTION_NAME,
                    InvocationType="Event",  # Asynchronous
                    Payload=json.dumps(payload),
                )
            except Exception as invoke_error:
                print(f"[ERROR] Invoke failed: {str(invoke_error)}")
                import traceback
                traceback.print_exc()
                raise

        return {
            "fetched_raw": len(items_raw),
            "fetched_stablehashtag": stablehashtag_posts_count,
            "skipped": total_skipped,
            "dense_posts": len(dense_texts),
            "total_base_forms": len(dense_base_forms)
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
