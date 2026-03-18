#!/usr/bin/env python3
"""
Publish feeds to Bluesky.
Creates feed generator records for both raw and dense feeds.
"""

import os
import sys
from datetime import datetime, timezone
from atproto import Client
from atproto_client.models.app.bsky.feed.generator import Record as GeneratorRecord
from atproto_client.models.com.atproto.repo.put_record import Data as PutData

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
        "displayName": "Japanese Raw Feed",
        "description": "日本語の[時系列順]フィード。※正常動作しますが、挙動を調整することがあります。",
        "avatar": "feed_icon_purple.png",
    },
    {
        "rkey": "japanese-dense-feed",
        "displayName": "Japanese Dense Feed",
        "description": "日本語の[時系列順／高密度／平穏]フィード。※正常動作しますが、挙動を調整することがあります。",
        "avatar": "feed_icon_green.png",
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
        avatar_file = feed_config["avatar"]

        print(f"\n  Publishing: {rkey}")
        print(f"    Display: {display_name}")
        print(f"    Desc: {description[:60]}...")

        try:
            # Upload avatar image if provided
            avatar_blob = None
            if avatar_file:
                # Use absolute path to scripts directory
                avatar_path = "/mnt/c/Users/k623m/bluesky-feed-jp/scripts/" + avatar_file
                if os.path.exists(avatar_path):
                    with open(avatar_path, "rb") as f:
                        avatar_data = f.read()
                    upload_response = client.upload_blob(avatar_data)
                    avatar_blob = upload_response.blob
                    print(f"    ✓ Avatar uploaded: {avatar_blob}")
                else:
                    print(f"    ✗ Avatar file not found")

            # Create record as dict with proper casing
            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            record_dict = {
                "did": "did:web:gtf03qzry3.execute-api.ap-northeast-1.amazonaws.com",
                "displayName": display_name,
                "description": description,
                "createdAt": now_iso,
            }

            # Add avatar if available (use by_alias=True for correct AT Protocol format)
            if avatar_blob:
                record_dict["avatar"] = avatar_blob.model_dump(by_alias=True)

            put_data = PutData(
                repo=user_did,
                collection="app.bsky.feed.generator",
                rkey=rkey,
                record=record_dict,
            )

            response = client.com.atproto.repo.put_record(put_data)

            feed_uri = f"at://{user_did}/app.bsky.feed.generator/{rkey}"
            print(f"    ✓ Published: {feed_uri}")

            # Also update profile to display the avatar
            if avatar_blob:
                print(f"    ℹ️  Avatar will be displayed on feed profile")

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
