import os
import json
import time
import boto3
from density_scorer import calculate_density_score

# Configuration
BSKY_HANDLE = os.environ.get("BSKY_HANDLE", "")
BSKY_APP_PASSWORD = os.environ.get("BSKY_APP_PASSWORD", "")
STORE_FUNCTION_NAME = os.environ.get("STORE_FUNCTION_NAME", "")
DENSITY_THRESHOLD = float(os.environ.get("DENSITY_THRESHOLD", "2.0"))

SEARCH_QUERY = "lang:ja"
LIMIT = 10

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

    model = get_language_model()
    prediction = model.predict(text.replace("\n", " "))
    label, confidence = prediction[0][0], prediction[1][0]

    # label format: '__label__ja'
    lang_code = label.replace("__label__", "")

    return lang_code == "ja" and confidence > 0.5

def has_any_labels(post):
    """Exclude posts that have any labels (moderation applied)"""
    labels = getattr(post, "labels", None)
    return bool(labels)

def lambda_handler(event, context):
    """
    Ingest Lambda: Fetches latest Japanese posts from Bluesky.

    Process:
    1. Search for posts with lang:ja
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

        # Search posts
        print(f"Searching for posts: {SEARCH_QUERY}")
        res = client.app.bsky.feed.search_posts({
            "q": SEARCH_QUERY,
            "sort": "latest",
            "limit": LIMIT,
        })

        # Process results
        items = []
        skipped_by_reason = {
            "invalid_fields": 0,
            "moderation_labels": 0,
            "non_japanese": 0,
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

            # Extract text
            record = getattr(post, "record", None)
            text = getattr(record, "text", "") if record else ""

            # Verify Japanese with fastText
            if not is_japanese(text):
                print(f"[FILTER] Non-Japanese: {uri}")
                skipped_by_reason["non_japanese"] += 1
                continue

            # Calculate density score
            density_score = calculate_density_score(text)

            # Convert indexed_at to timestamp
            ts = time.mktime(time.strptime(indexed_at, "%Y-%m-%dT%H:%M:%S.%fZ"))

            items.append({
                "uri": uri,
                "ts": ts,
                "density_score": density_score,
            })

            print(f"[ADDED] {uri} (density={density_score:.3f})")

        print(f"\n=== Processing Summary ===")
        print(f"Total fetched: {len(posts)}")
        print(f"  - Invalid fields: {skipped_by_reason['invalid_fields']}")
        print(f"  - Moderation labels: {skipped_by_reason['moderation_labels']}")
        print(f"  - Non-Japanese: {skipped_by_reason['non_japanese']}")
        print(f"  - Passed filters: {len(items)}")
        print(f"=========================\n")

        # Calculate total skipped
        total_skipped = sum(skipped_by_reason.values())

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
