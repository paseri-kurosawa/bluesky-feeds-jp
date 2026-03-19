#!/usr/bin/env python3
"""
Delete feeds from Bluesky.
Removes feed generator records for both raw and dense feeds.
"""

import os
import sys
from atproto import Client
from atproto_client.models.com.atproto.repo.delete_record import Data as DeleteData

# Bluesky credentials for the old account
BSKY_HANDLE = "example.bsky.social"
BSKY_APP_PASSWORD = "747w-ay65-5fv6-3vci"

# Feed rkeys to delete
FEED_RKEYS = [
    "japanese-raw-feed",
    "japanese-dense-feed",
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
