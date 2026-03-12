#!/usr/bin/env python3
"""
Publish feeds to Bluesky.
Creates feed generator records for both raw and dense feeds.
"""

import os
import sys
from datetime import datetime, timezone
from atproto import Client

# Load environment variables
BSKY_HANDLE = os.environ.get("BSKY_HANDLE", "")
BSKY_APP_PASSWORD = os.environ.get("BSKY_APP_PASSWORD", "")

if not BSKY_HANDLE or not BSKY_APP_PASSWORD:
    print("❌ Error: BSKY_HANDLE and BSKY_APP_PASSWORD must be set in .env")
    sys.exit(1)

# Feed configuration
FEEDS = [
    {
        "rkey": "japanese-raw-feed",
        "displayName": "JP Raw (PoC)",
        "description": "[EXPERIMENTAL PoC] Japanese posts (lang:jp + fasttext verified). Latest first. DO NOT USE FOR PRODUCTION.",
        "avatar": None,
    },
    {
        "rkey": "japanese-dense-feed",
        "displayName": "JP Dense (PoC)",
        "description": "[EXPERIMENTAL PoC] High-density Japanese posts (Janome analysis + score >= 2.0). Latest first. DO NOT USE FOR PRODUCTION.",
        "avatar": None,
    },
]

def publish_feeds():
    """Publish feeds to Bluesky."""
    print("🔐 Authenticating with Bluesky...")
    client = Client()
    client.login(BSKY_HANDLE, BSKY_APP_PASSWORD)

    profile = client.get_profile(BSKY_HANDLE)
    user_did = profile.did
    print(f"✓ Authenticated as: {BSKY_HANDLE} ({user_did})")

    print(f"\n📢 Publishing {len(FEEDS)} feeds...")

    for feed_config in FEEDS:
        rkey = feed_config["rkey"]
        display_name = feed_config["displayName"]
        description = feed_config["description"]

        print(f"\n  Publishing: {rkey}")
        print(f"    Display: {display_name}")
        print(f"    Desc: {description[:60]}...")

        try:

            # Create new feed generator record
            from atproto_client.models.com.atproto.repo.create_record import Data

            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            record = {
                "did": "did:web:gtf03qzry3.execute-api.ap-northeast-1.amazonaws.com",
                "displayName": display_name,
                "description": description,
                "createdAt": now_iso,
            }

            create_data = Data(
                repo=user_did,
                collection="app.bsky.feed.generator",
                rkey=rkey,
                record=record,
            )

            response = client.com.atproto.repo.create_record(create_data)

            feed_uri = f"at://{user_did}/app.bsky.feed.generator/{rkey}"
            print(f"    ✓ Published: {feed_uri}")

        except Exception as e:
            print(f"    ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return False

    print("\n✅ All feeds published successfully!")
    return True

if __name__ == "__main__":
    success = publish_feeds()
    sys.exit(0 if success else 1)
