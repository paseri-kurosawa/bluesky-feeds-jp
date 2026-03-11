#!/usr/bin/env python3
"""
Extract posts from Valkey dense feed and save texts to S3.
This Lambda runs within VPC, allowing connection to Valkey Serverless.
"""

import os
import json
import time
import boto3
import redis
from datetime import datetime
from atproto import Client

# Environment variables
BSKY_HANDLE = os.environ.get("BSKY_HANDLE", "")
BSKY_APP_PASSWORD = os.environ.get("BSKY_APP_PASSWORD", "")
VALKEY_ENDPOINT = os.environ.get("VALKEY_ENDPOINT", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "badword-analysis")

def lambda_handler(event, context):
    """
    Extract dense feed posts and save to S3.

    Output format: One post text per line (newlines escaped as \\n)
    File location: s3://{S3_BUCKET}/{S3_PREFIX}/dense_posts_{YYYYMMDD_HHMMSS}.txt
    """
    try:
        print(f"Starting post extraction from dense feed...")
        print(f"Valkey endpoint: {VALKEY_ENDPOINT}")

        # Connect to Valkey
        print("Connecting to Valkey...")
        valkey = redis.Redis(
            host=VALKEY_ENDPOINT,
            port=6379,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_keepalive=True,
        )

        try:
            valkey.ping()
            print("✓ Connected to Valkey")
        except Exception as e:
            print(f"✗ Valkey connection failed: {e}")
            return {
                "statusCode": 500,
                "error": f"Valkey connection failed: {str(e)}",
            }

        # Get all URIs from dense feed
        print("Retrieving URIs from feed:dense:jp:v1...")
        uris = valkey.zrevrange("feed:dense:jp:v1", 0, -1)
        print(f"✓ Found {len(uris)} URIs in dense feed")

        if not uris:
            print("No posts found in dense feed")
            return {
                "statusCode": 200,
                "extracted": 0,
                "errors": 0,
                "message": "No posts in dense feed",
            }

        # Authenticate with Bluesky
        print(f"Logging in to Bluesky as {BSKY_HANDLE}...")
        client = Client()
        client.login(BSKY_HANDLE, BSKY_APP_PASSWORD)
        print("✓ Logged in to Bluesky")

        # Extract post texts
        print(f"\nExtracting {len(uris)} post texts...")
        texts = []
        errors = []

        for idx, uri in enumerate(uris):
            try:
                # Parse URI to get repo and rkey
                # Format: at://did:plc:xxx/app.bsky.feed.post/xxxxx
                parts = uri.split("/")
                if len(parts) < 4:
                    errors.append(f"Invalid URI format: {uri}")
                    continue

                repo = parts[2]
                rkey = parts[-1]

                # Fetch record from Bluesky
                record = client.com.atproto.repo.get_record(
                    repo=repo,
                    collection="app.bsky.feed.post",
                    rkey=rkey
                )

                # Extract text
                if record and hasattr(record, "value") and hasattr(record.value, "text"):
                    text = record.value.text
                    # Escape newlines and carriage returns for single-line format
                    text_escaped = text.replace("\n", "\\n").replace("\r", "\\r")
                    texts.append(text_escaped)

                    if (idx + 1) % 10 == 0:
                        print(f"  Extracted {idx + 1}/{len(uris)} posts...")
                else:
                    errors.append(f"No text found: {uri}")

            except Exception as e:
                errors.append(f"Error fetching {uri}: {str(e)}")

        print(f"\n✓ Extraction complete!")
        print(f"  - Successfully extracted: {len(texts)} posts")
        print(f"  - Errors: {len(errors)}")

        if errors:
            print(f"  - First 5 errors:")
            for err in errors[:5]:
                print(f"    - {err}")

        # Save to S3
        if texts:
            print(f"\nSaving to S3...")
            s3 = boto3.client("s3")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"{S3_PREFIX}/dense_posts_{timestamp}.txt"

            # Join texts with newlines
            content = "\n".join(texts)

            try:
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=content.encode("utf-8"),
                    ContentType="text/plain; charset=utf-8",
                )
                print(f"✓ Saved to s3://{S3_BUCKET}/{s3_key}")
                s3_url = f"s3://{S3_BUCKET}/{s3_key}"
            except Exception as e:
                print(f"✗ S3 upload failed: {e}")
                return {
                    "statusCode": 500,
                    "error": f"S3 upload failed: {str(e)}",
                    "extracted": len(texts),
                    "errors": len(errors),
                }
        else:
            print("No texts to save (all failed)")
            s3_url = None

        return {
            "statusCode": 200,
            "extracted": len(texts),
            "errors": len(errors),
            "s3_url": s3_url,
            "total_uris": len(uris),
        }

    except Exception as e:
        print(f"Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "error": str(e),
        }
