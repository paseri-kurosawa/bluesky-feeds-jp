#!/usr/bin/env python3
"""
Delete feeds from Bluesky.
Removes feed generator records for both raw and dense feeds.
"""

import os
import sys
from dotenv import load_dotenv
from atproto import Client
from atproto_client.models.com.atproto.repo.delete_record import Data as DeleteData

# Load environment variables from .env
load_dotenv()

BSKY_HANDLE = os.environ.get("BSKY_HANDLE", "")
BSKY_APP_PASSWORD = os.environ.get("BSKY_APP_PASSWORD", "")

# If not in .env, try to load from AWS Secrets Manager
if not BSKY_HANDLE or not BSKY_APP_PASSWORD:
    import boto3
    import json
    try:
        print("Loading Bluesky credentials from AWS Secrets Manager...")
        sm_client = boto3.client("secretsmanager", region_name="ap-northeast-1")
        response = sm_client.get_secret_value(SecretId="bluesky-feed-jp/credentials")
        secret = json.loads(response["SecretString"])
        BSKY_HANDLE = secret.get("handle", BSKY_HANDLE)
        BSKY_APP_PASSWORD = secret.get("appPassword", BSKY_APP_PASSWORD)
    except Exception as e:
        print(f"❌ Error loading from Secrets Manager: {e}")

if not BSKY_HANDLE or not BSKY_APP_PASSWORD:
    print("❌ Error: BSKY_HANDLE and BSKY_APP_PASSWORD must be set in .env or Secrets Manager")
    sys.exit(1)

# Feed rkeys to delete
FEED_RKEYS = [
    "japanese-raw-feed",
    "japanese-dense-feed",
    "japanese-stablehashtag-feed",
]

def delete_feeds():
    """Delete feeds from Bluesky."""
    print("🔐 Authenticating with Bluesky...")
    client = Client()
    client.login(BSKY_HANDLE, BSKY_APP_PASSWORD)

    profile = client.get_profile(BSKY_HANDLE)
    user_did = profile.did
    print(f"✓ Authenticated as: {BSKY_HANDLE} ({user_did})")

    print(f"\n🗑️  Deleting {len(FEED_RKEYS)} feeds...")

    for rkey in FEED_RKEYS:
        print(f"\n  Deleting: {rkey}")

        try:
            # Delete the feed record
            delete_data = DeleteData(
                repo=user_did,
                collection="app.bsky.feed.generator",
                rkey=rkey,
            )
            client.com.atproto.repo.delete_record(delete_data)

            feed_uri = f"at://{user_did}/app.bsky.feed.generator/{rkey}"
            print(f"    ✓ Deleted: {feed_uri}")

        except Exception as e:
            print(f"    ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return False

    print("\n✅ All feeds deleted successfully!")
    return True

if __name__ == "__main__":
    success = delete_feeds()
    sys.exit(0 if success else 1)
