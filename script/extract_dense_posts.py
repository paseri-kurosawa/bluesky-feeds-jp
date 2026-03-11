#!/usr/bin/env python3
"""
Extract posts from Valkey dense feed and save text to a file.
"""

import os
import json
import sys
from datetime import datetime
import redis
from atproto import Client

# Load environment variables
BSKY_HANDLE = os.getenv("BSKY_HANDLE")
BSKY_APP_PASSWORD = os.getenv("BSKY_APP_PASSWORD")
VALKEY_ENDPOINT = os.getenv("VALKEY_ENDPOINT")

if not BSKY_HANDLE or not BSKY_APP_PASSWORD or not VALKEY_ENDPOINT:
    print("Error: Missing required environment variables")
    print("Required: BSKY_HANDLE, BSKY_APP_PASSWORD, VALKEY_ENDPOINT")
    sys.exit(1)

# Initialize Bluesky client
print(f"Logging in to Bluesky as {BSKY_HANDLE}...")
client = Client()
client.login(BSKY_HANDLE, BSKY_APP_PASSWORD)
print("✓ Logged in to Bluesky")

# Connect to Valkey
print(f"Connecting to Valkey: {VALKEY_ENDPOINT}...")
valkey = redis.Redis(host=VALKEY_ENDPOINT, port=6379, decode_responses=True)
try:
    valkey.ping()
    print("✓ Connected to Valkey")
except Exception as e:
    print(f"Error connecting to Valkey: {e}")
    sys.exit(1)

# Get all URIs from dense feed (sorted by timestamp, newest first)
print("Retrieving URIs from feed:dense:jp:v1...")
uris = valkey.zrevrange("feed:dense:jp:v1", 0, -1)
print(f"✓ Found {len(uris)} URIs in dense feed")

if not uris:
    print("No posts found in dense feed")
    sys.exit(0)

# Create output directory if it doesn't exist
output_dir = os.path.dirname(os.path.abspath(__file__)) + "/work"
os.makedirs(output_dir, exist_ok=True)

# Generate output filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = os.path.join(output_dir, f"dense_posts_{timestamp}.txt")

print(f"\nExtracting post texts...")
extracted_count = 0
error_count = 0

with open(output_file, "w", encoding="utf-8") as f:
    for uri in uris:
        try:
            # Parse URI to get repo and collection/rkey
            # Format: at://did:plc:xxx/app.bsky.feed.post/xxxxx
            parts = uri.split("/")
            if len(parts) < 4:
                print(f"Warning: Invalid URI format: {uri}")
                error_count += 1
                continue

            repo = parts[2]
            rkey = parts[-1]

            # Fetch post using getRecord
            record = client.com.atproto.repo.get_record(
                repo=repo,
                collection="app.bsky.feed.post",
                rkey=rkey
            )

            # Extract text
            if record and hasattr(record, "value") and hasattr(record.value, "text"):
                text = record.value.text
                # Write as single line (escape newlines)
                text_single_line = text.replace("\n", "\\n").replace("\r", "\\r")
                f.write(text_single_line + "\n")
                extracted_count += 1
            else:
                print(f"Warning: Could not extract text from {uri}")
                error_count += 1

        except Exception as e:
            print(f"Error fetching {uri}: {e}")
            error_count += 1

print(f"\n✓ Extraction complete!")
print(f"  - Successfully extracted: {extracted_count} posts")
print(f"  - Errors: {error_count}")
print(f"  - Output file: {output_file}")
