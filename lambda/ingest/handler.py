import os
import json
import time
import boto3
import unicodedata
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta
from density_scorer import calculate_density_score, tokenize_japanese, extract_base_forms

# Japan Standard Time (UTC+9)
JST = timezone(timedelta(hours=9))

# AWS Clients
cloudwatch_client = boto3.client("cloudwatch")
logs_client = boto3.client("logs")
s3_client = boto3.client("s3")

# CloudWatch Logs query function for feed-specific calls
def get_getfeed_calls_by_feed_type(target_date):
    """
    Query API Gateway access logs from CloudWatch Logs to count GetFeed calls by feed type.

    Args:
        target_date: Date string in format YYYY-MM-DD

    Returns:
        Dict with raw_calls, dense_calls, stablehashtag_calls, total_invocations
    """
    try:
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        start_time = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)

        # Convert to Unix timestamp in seconds
        start_time_s = int(start_time.timestamp())
        end_time_s = int(end_time.timestamp())

        log_group = '/aws/apigateway/bluesky-feed-jp'

        print(f"[LOGS] Querying API Gateway access logs for {target_date}")

        # Query for each feed type
        query_patterns = {
            'raw_calls': 'feed=raw',
            'dense_calls': 'feed=dense',
            'stablehashtag_calls': 'feed=stablehashtag'
        }

        results = {'total_invocations': 0}

        for field_name, pattern in query_patterns.items():
            try:
                query_string = f'fields @timestamp | filter @message like /{pattern}/ | stats count() as count'
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
                        results['total_invocations'] += count
                        break
                    time.sleep(1)
            except Exception as e:
                print(f"[LOGS] Error querying {field_name}: {str(e)}")
                results[field_name] = 0

        print(f"[LOGS] Feed calls for {target_date}: raw={results.get('raw_calls', 0)}, dense={results.get('dense_calls', 0)}, stablehashtag={results.get('stablehashtag_calls', 0)}")
        return results

    except Exception as e:
        print(f"[LOGS] Error querying feed calls for {target_date}: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'raw_calls': 0,
            'dense_calls': 0,
            'stablehashtag_calls': 0,
            'total_invocations': 0
        }

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


def get_rotation_state(bucket):
    """Load rotation state from S3"""
    s3_client = boto3.client("s3")
    state_key = "hashtags/rotation/state.json"

    try:
        response = s3_client.get_object(Bucket=bucket, Key=state_key)
        state = json.loads(response["Body"].read().decode("utf-8"))
        return state
    except Exception as e:
        print(f"[ROTATION] Error loading state: {e}, initializing with default")
        return {
            "current_index": 0,
            "last_rotation_time": datetime.now(JST).isoformat(),
            "total_rotations": 0,
            "stable_hashtags": [],
            "hot_fired_at_index": []
        }


def save_rotation_state(bucket, state):
    """Save updated rotation state to S3"""
    s3_client = boto3.client("s3")
    state_key = "hashtags/rotation/state.json"

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=state_key,
            Body=json.dumps(state, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"[ROTATION] Updated state: index={state['current_index']}, rotations={state['total_rotations']}")
    except Exception as e:
        print(f"[ROTATION] Error saving state: {e}")


def get_stable_hashtags(bucket):
    """Load stable hashtags from S3 components directory"""
    s3_client = boto3.client("s3")
    hashtags_key = "components/stable_hashtags.json"

    try:
        response = s3_client.get_object(Bucket=bucket, Key=hashtags_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        tags = data.get("top_hashtags", [])
        return tags
    except Exception as e:
        print(f"[HASHTAGS] Error loading stable hashtags from {hashtags_key}: {e}")
        return []


def get_1h_hashtags(bucket):
    """Load 1H hashtags from S3 components directory"""
    try:
        hashtags_key = "components/top_hashtags_1h_from_raw_posts.json"
        response = s3_client.get_object(Bucket=bucket, Key=hashtags_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        hashtags_1h = data.get("top_hashtags_1h", [])
        # Convert list of dicts to set of tag names
        tags_1h_set = {tag_dict["tag"].lower() for tag_dict in hashtags_1h}
        print(f"[1H HASHTAGS] Loaded {len(tags_1h_set)} unique 1H hashtags")
        return tags_1h_set
    except Exception as e:
        print(f"[1H HASHTAGS] Error loading 1H hashtags: {e}")
        return set()


def get_current_hashtag(bucket):
    """
    Get current hashtag based on rotation state with hot detection.

    Flow:
    1. Load rotation state
    2. Load 1H hashtags
    3. Detect hot hashtags (intersection of stable + 1H)
    4. Check if hot hashtag is in cooldown (hot_fired_at_index)
    5. If hot detected, use it and don't advance current_index
    6. If no hot, use normal rotation and advance current_index
    7. Clean up cooled-down hot tags (when current_index cycles back)
    """
    config = get_config()
    top_n = config.get("hashtag_rotation", {}).get("top_n", 5)

    state = get_rotation_state(bucket)
    tags = state.get("stable_hashtags", [])

    if not tags:
        print("[HASHTAGS] No stable tags available in rotation state")
        return None, state

    # Use only top_n tags from config
    active_tags = tags[:top_n]
    current_index = state.get("current_index", 0) % len(active_tags)
    hot_fired_at_index = state.get("hot_fired_at_index", [])

    print(f"[ROTATION] Current index: {current_index}, Hot cooldown: {hot_fired_at_index}")

    # Load 1H hashtags for hot detection
    tags_1h_set = get_1h_hashtags(bucket)

    # === Hot Detection & Cooldown Management ===
    current_tag = None
    next_index = current_index
    next_hot_fired_at_index = hot_fired_at_index.copy()
    hot_was_fired = False

    # Check for hot hashtags in active_tags[:top_n]
    for idx, tag_dict in enumerate(active_tags):
        tag_name = tag_dict["tag"].lower()

        # Check if tag is in 1H hashtags (hot)
        if tag_name in tags_1h_set:
            # Check if already in cooldown
            in_cooldown = any(entry.get("tag", "").lower() == tag_name for entry in hot_fired_at_index)

            if not in_cooldown:
                # Hot detected and not in cooldown
                print(f"[FIRE] Hot tag detected: #{tag_dict['tag']} (index={current_index})")
                current_tag = tag_dict["tag"]
                hot_was_fired = True
                next_hot_fired_at_index.append({
                    "tag": tag_dict["tag"],
                    "index": current_index,
                    "fired_at": datetime.now(JST).isoformat()
                })
                # DO NOT advance current_index (stay at current)
                next_index = current_index
                break
            else:
                print(f"[COOLDOWN] Hot tag in cooldown: #{tag_dict['tag']} (index={idx})")

    # === Normal Rotation (if no hot tag) ===
    if not hot_was_fired:
        current_tag = active_tags[current_index]["tag"]
        # Advance current_index only if no hot tag was used
        next_index = (current_index + 1) % len(active_tags)
        print(f"[ROTATION] Normal rotation: #{current_tag} (index={current_index})")

    # === Cooldown Cleanup ===
    # If no hot tag was used this batch, check if next_index matches any fired_at_index
    # (execute after rotation to use updated next_index for cleanup)
    if not hot_was_fired:
        # Remove from cooldown if next_index cycles back
        prev_len = len(next_hot_fired_at_index)
        next_hot_fired_at_index = [
            entry
            for entry in next_hot_fired_at_index
            if entry.get("index") != next_index
        ]

        if len(next_hot_fired_at_index) < prev_len:
            removed_tags = [entry.get("tag") for entry in hot_fired_at_index if entry.get("index") == next_index]
            print(f"[COOLDOWN RELEASE] Tags released from cooldown: {removed_tags}")

    # Prepare next state
    next_state = {
        "current_index": next_index,
        "last_rotation_time": datetime.now(JST).isoformat(),
        "total_rotations": state.get("total_rotations", 0) + 1,
        "stable_hashtags": tags,
        "hot_fired_at_index": next_hot_fired_at_index
    }

    print(f"[ROTATION] Next state: index={next_index}, hot_fired_at_index={next_hot_fired_at_index}")

    return current_tag, next_state


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

        # === QUERY 2: lang:ja #<stable_hashtag> (STABLETAG) ===
        current_hashtag, next_state = get_current_hashtag(statistics_bucket)
        items_stablehashtag = []
        stablehashtag_posts_count = 0

        if current_hashtag:
            search_query_2 = f"lang:ja #{current_hashtag}"
            try:
                res_2 = search_posts_with_retry(client, search_query_2, search_config, max_retries=3)
                posts_2 = getattr(res_2, "posts", []) or []
                print(f"[QUERY2] Found {len(posts_2)} posts for: {search_query_2}")

                items_stablehashtag, dense_texts_stablehashtag, dense_base_forms_stablehashtag, badword_stats_stablehashtag, skipped_by_reason_stablehashtag = process_posts_with_filters(posts_2, feed_type="stablehashtag")
                stablehashtag_posts_count = len(posts_2)

                # Update rotation state
                save_rotation_state(statistics_bucket, next_state)
            except Exception as e:
                print(f"[QUERY2] Error fetching stablehashtag posts: {e}")
        else:
            print("[QUERY2] No current hashtag available, skipping stablehashtag query")

        # Combine items for statistics (raw+stablehashtag)
        items = items_raw + items_stablehashtag

        print(f"\n=== Processing Summary ===")
        print(f"Total fetched (raw+stablehashtag): {len(posts_1) + stablehashtag_posts_count}")
        print(f"  - Raw posts: {len(posts_1)}")
        print(f"  - Stabletag posts: {stablehashtag_posts_count}")
        print(f"  - Invalid fields: {skipped_by_reason['invalid_fields']}")
        print(f"  - Moderation labels: {skipped_by_reason['moderation_labels']}")
        print(f"  - Non-Japanese: {skipped_by_reason['non_japanese']}")
        print(f"  - Spam hashtags (5+): {skipped_by_reason['spam_hashtags']}")
        print(f"  - Passed filters: {len(items_raw + items_stablehashtag)}")

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
            "version": "1.0"
        }

        # Extract and count hashtags from Raw feed only
        # Normalize tags (Unicode NFC + lowercase) to absorb variation
        hashtag_counts = {}
        for item in items_raw:
            hashtags = item.get("hashtags", [])
            for tag in hashtags:
                normalized_tag = unicodedata.normalize("NFC", tag).lower()
                hashtag_counts[normalized_tag] = hashtag_counts.get(normalized_tag, 0) + 1

        # Check previous day's daily file and query CloudWatch Logs if needed
        now_jst = datetime.now(JST)
        yesterday = now_jst - timedelta(days=1)
        yesterday_date = yesterday.strftime("%Y-%m-%d")

        getfeed_stats = {
            "raw_calls": 0,
            "dense_calls": 0,
            "stablehashtag_calls": 0,
            "total_invocations": 0
        }

        # Check if yesterday's daily file exists in STATISTICS_BUCKET
        try:
            s3_client = boto3.client("s3")
            statistics_bucket = os.environ.get("STATISTICS_BUCKET", "")
            daily_key = f"stats/daily/stats-{yesterday_date}.json"
            s3_client.head_object(Bucket=statistics_bucket, Key=daily_key)
        except ClientError as e:
            # Check if it's a 404 (NoSuchKey)
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                # Daily file doesn't exist, query CloudWatch Logs for feed-specific calls
                getfeed_stats = get_getfeed_calls_by_feed_type(yesterday_date)
        except Exception as e:
            pass

        # Invoke Store Lambda asynchronously
        if items_raw or items_stablehashtag:
            lambda_client = boto3.client("lambda")
            config = get_config()
            top_n = config.get("hashtag_rotation", {}).get("top_n", 5)
            payload = {
                "items_raw": items_raw,
                "items_stablehashtag": items_stablehashtag,
                "batch_stats_raw": stats_payload_raw,
                "batch_stats_stablehashtag": stats_payload_stablehashtag,
                "dense_texts": dense_texts,
                "dense_texts_stablehashtag": dense_texts_stablehashtag,
                "dense_base_forms": dense_base_forms,
                "dense_base_forms_stablehashtag": dense_base_forms_stablehashtag,
                "getfeed_stats": getfeed_stats,
                "hashtags": hashtag_counts,
                "top_n": top_n
            }

            try:
                response = lambda_client.invoke(
                    FunctionName=STORE_FUNCTION_NAME,
                    InvocationType="Event",  # Asynchronous
                    Payload=json.dumps(payload),
                )
                print(f"[DEBUG_INVOKE] SUCCESS! StatusCode={response['StatusCode']}, RequestId={response.get('ResponseMetadata', {}).get('RequestId', 'N/A')}")
            except Exception as invoke_error:
                print(f"[DEBUG_INVOKE] ERROR: {str(invoke_error)}")
                import traceback
                traceback.print_exc()
                raise
        else:
            print(f"[DEBUG_INVOKE] SKIPPED: No items to invoke")

        return {
            "fetched_raw": len(items_raw),
            "fetched_stablehashtag": stablehashtag_posts_count,
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
