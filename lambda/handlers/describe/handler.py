import json
import os

def lambda_handler(event, context):
    """
    Describe Feed Generator endpoint: /xrpc/app.bsky.feed.describeFeedGenerator
    Bluesky calls this to get feed metadata.
    """
    feed_did = os.environ.get('FEED_DID', 'did:web:example.com')

    # Feed URIs for example.bsky.social
    raw_feed_uri = "at://did:plc:p5lasawfxrns4xs7646gc3hp/app.bsky.feed.generator/japanese-raw-feed"
    dense_feed_uri = "at://did:plc:p5lasawfxrns4xs7646gc3hp/app.bsky.feed.generator/japanese-dense-feed"

    description = {
        "did": feed_did,
        "feeds": [
            {
                "uri": raw_feed_uri,
                "displayName": "[PoC] Japanese Raw Feed",
                "description": "[EXPERIMENTAL PoC] Japanese posts (lang:jp + fasttext verified). Latest first. DO NOT USE FOR PRODUCTION."
            },
            {
                "uri": dense_feed_uri,
                "displayName": "[PoC] Japanese Dense Feed",
                "description": "[EXPERIMENTAL PoC] High-density Japanese posts (Janome analysis + score >= 2.0). Latest first. DO NOT USE FOR PRODUCTION."
            }
        ]
    }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(description)
    }
