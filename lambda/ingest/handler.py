import os
import json
import time
import boto3
from datetime import datetime
from density_scorer import calculate_density_score, tokenize_japanese, extract_base_forms

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

# Environment variables (credentials only)
BSKY_HANDLE = os.environ.get("BSKY_HANDLE", "")
BSKY_APP_PASSWORD = os.environ.get("BSKY_APP_PASSWORD", "")
STORE_FUNCTION_NAME = os.environ.get("STORE_FUNCTION_NAME", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")

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
            feature_type = getattr(feature, "$type", None)
            # Hashtag facet type is "app.bsky.richtext.facet#tag"
            if feature_type and "tag" in feature_type:
                hashtag_count += 1

    return hashtag_count

def save_statistics_report(statistics_bucket, skipped_by_reason, items, badword_stats, dense_texts, dense_rate, posts):
    """
    Generate and save statistics report as Markdown to S3.

    Args:
        statistics_bucket: S3 bucket name for statistics
        skipped_by_reason: Dict of skip reasons
        items: List of processed items
        badword_stats: Dict of badword statistics
        dense_texts: List of dense feed texts
        dense_rate: Dense feed rate (%)
        posts: List of fetched posts
    """
    if not statistics_bucket:
        return

    try:
        s3_client = boto3.client("s3")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        iso_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Calculate badword hit rate
        badword_hit_rate = (badword_stats['total_posts_with_badwords'] / len(items) * 100) if items else 0
        avg_matches = (badword_stats['total_badword_matches'] / badword_stats['total_posts_with_badwords']
                      if badword_stats['total_posts_with_badwords'] > 0 else 0)

        # Generate markdown
        markdown_content = f"""# Ingest Statistics Report
**Execution Time:** {iso_timestamp}

## Processing Summary
| Item | Count | Rate |
|------|-------|------|
| Total Fetched | {len(posts)} | 100.0% |
| Invalid Fields | {skipped_by_reason['invalid_fields']} | {skipped_by_reason['invalid_fields']/len(posts)*100:.1f}% |
| Moderation Labels | {skipped_by_reason['moderation_labels']} | {skipped_by_reason['moderation_labels']/len(posts)*100:.1f}% |
| Non-Japanese | {skipped_by_reason['non_japanese']} | {skipped_by_reason['non_japanese']/len(posts)*100:.1f}% |
| **Passed Filters** | **{len(items)}** | **{len(items)/len(posts)*100:.1f}%** |

## Badword Analysis
| Metric | Value |
|--------|-------|
| Posts with Badwords | {badword_stats['total_posts_with_badwords']} |
| Hit Rate | {badword_hit_rate:.1f}% |
| Total Matches | {badword_stats['total_badword_matches']} |
| Avg Matches per Hit | {avg_matches:.2f} |

## Dense Feed Statistics
| Metric | Value |
|--------|-------|
| Dense Posts | {len(dense_texts)} |
| Total Items | {len(items)} |
| Dense Rate | {dense_rate:.1f}% |

---
*Report generated by Ingest Lambda*
"""

        s3_key = f"stats/stats_{timestamp}.md"
        s3_client.put_object(
            Bucket=statistics_bucket,
            Key=s3_key,
            Body=markdown_content.encode("utf-8"),
            ContentType="text/markdown; charset=utf-8",
        )
        print(f"[STATISTICS] Saved report to s3://{statistics_bucket}/{s3_key}")
        return f"s3://{statistics_bucket}/{s3_key}"

    except Exception as e:
        print(f"[STATISTICS ERROR] Failed to save statistics: {str(e)}")
        return None

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
        except InvokeTimeoutError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # exponential backoff: 1s, 2s, 4s
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

        # Authentication
        print("Authenticating with Bluesky...")
        client = Client()
        client.login(BSKY_HANDLE, BSKY_APP_PASSWORD)

        # Search posts with retry logic
        config = get_config()
        search_config = config["search"]
        res = search_posts_with_retry(client, search_config, max_retries=3)

        # Process results
        items = []
        dense_texts = []  # Collect texts for Dense feed
        dense_base_forms = []  # Collect base forms (見出し語) for badword dictionary creation
        skipped_by_reason = {
            "invalid_fields": 0,
            "moderation_labels": 0,
            "non_japanese": 0,
        }
        badword_stats = {
            "total_posts_with_badwords": 0,
            "total_badword_matches": 0,
            "badword_distribution": {},  # Count by number of matches per post
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

            # Check for images in embed
            has_images = False
            embed = getattr(record, "embed", None) if record else None
            if embed:
                # Check for images field (varies by embed type)
                images = getattr(embed, "images", None)
                has_images = bool(images)

            # Extract hashtag count from facets
            hashtag_count = extract_hashtag_count(record)

            # Calculate density score with attributes
            density_score, badword_count = calculate_density_score(text, is_reply=is_reply, has_images=has_images, hashtag_count=hashtag_count)

            # Record badword statistics
            if badword_count > 0:
                badword_stats["total_posts_with_badwords"] += 1
                badword_stats["total_badword_matches"] += badword_count
                # Track distribution of matches per post
                key = str(badword_count)
                badword_stats["badword_distribution"][key] = badword_stats["badword_distribution"].get(key, 0) + 1

            # Convert indexed_at to timestamp
            ts = time.mktime(time.strptime(indexed_at, "%Y-%m-%dT%H:%M:%S.%fZ"))

            items.append({
                "uri": uri,
                "ts": ts,
                "density_score": density_score,
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

        # Save dense texts to S3
        s3_url = None
        s3_base_forms_url = None
        if dense_texts and S3_BUCKET:
            try:
                s3_client = boto3.client("s3")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                # Save raw texts
                s3_key = f"badword-analysis/dense_posts_{timestamp}.txt"
                content = "\n".join(dense_texts)

                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=content.encode("utf-8"),
                    ContentType="text/plain; charset=utf-8",
                )
                s3_url = f"s3://{S3_BUCKET}/{s3_key}"
                print(f"[S3] Saved {len(dense_texts)} dense post texts to {s3_url}")

                # Save base forms (見出し語) - 1 word per line
                if dense_base_forms:
                    s3_base_forms_key = f"badword-analysis/dense_posts_base_forms_{timestamp}.txt"
                    base_forms_content = "\n".join(dense_base_forms)

                    s3_client.put_object(
                        Bucket=S3_BUCKET,
                        Key=s3_base_forms_key,
                        Body=base_forms_content.encode("utf-8"),
                        ContentType="text/plain; charset=utf-8",
                    )
                    s3_base_forms_url = f"s3://{S3_BUCKET}/{s3_base_forms_key}"
                    print(f"[S3] Saved {len(dense_base_forms)} base forms to {s3_base_forms_url}")

            except Exception as e:
                print(f"[S3 ERROR] Failed to save dense texts: {str(e)}")

        # Save statistics report
        statistics_bucket = os.environ.get("STATISTICS_BUCKET", "")
        statistics_url = save_statistics_report(statistics_bucket, skipped_by_reason, items, badword_stats, dense_texts, dense_rate, posts)

        # Invoke Store Lambda asynchronously
        if items:
            lambda_client = boto3.client("lambda")
            payload = {"items": items}

            response = lambda_client.invoke(
                FunctionName=STORE_FUNCTION_NAME,
                InvocationType="Event",  # Asynchronous
                Payload=json.dumps(payload),
            )
            print(f"Store Lambda invoked: {response['StatusCode']}")

        return {
            "fetched": len(items),
            "skipped": total_skipped,
            "dense_posts": len(dense_texts),
            "total_base_forms": len(dense_base_forms),
            "s3_url": s3_url,
            "s3_base_forms_url": s3_base_forms_url,
            "statistics_url": statistics_url,
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
