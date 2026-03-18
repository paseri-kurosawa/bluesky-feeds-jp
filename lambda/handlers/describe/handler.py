import json
import os
import base64

def load_image_as_data_url(filename):
    """Load image file and convert to data URL"""
    file_path = os.path.join(os.path.dirname(__file__), filename)
    try:
        with open(file_path, 'rb') as f:
            image_data = f.read()
        b64_data = base64.b64encode(image_data).decode('utf-8')
        return f"data:image/png;base64,{b64_data}"
    except Exception as e:
        print(f"Failed to load image {filename}: {e}")
        return None

def lambda_handler(event, context):
    """
    Describe Feed Generator endpoint: /xrpc/app.bsky.feed.describeFeedGenerator
    Bluesky calls this to get feed metadata.
    """
    feed_did = os.environ.get('FEED_DID', 'did:web:example.com')

    # Feed URIs for example.bsky.social
    raw_feed_uri = "at://did:plc:p5lasawfxrns4xs7646gc3hp/app.bsky.feed.generator/japanese-raw-feed"
    dense_feed_uri = "at://did:plc:p5lasawfxrns4xs7646gc3hp/app.bsky.feed.generator/japanese-dense-feed"

    # Load images as data URLs
    blue_image = load_image_as_data_url("feed_image_blue.png")
    green_image = load_image_as_data_url("feed_image_green.png")

    description = {
        "did": feed_did,
        "feeds": [
            {
                "uri": raw_feed_uri,
                "displayName": "Japanese Raw Feed",
                "description": "日本語の[時系列順]フィード。※正常動作しますが、挙動を調整することがあります。",
                "avatar": blue_image
            },
            {
                "uri": dense_feed_uri,
                "displayName": "Japanese Dense Feed",
                "description": "日本語の[時系列順／高密度／平穏]フィード。※正常動作しますが、挙動を調整することがあります。",
                "avatar": green_image
            }
        ]
    }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(description)
    }
