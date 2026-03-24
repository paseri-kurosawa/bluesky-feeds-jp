#!/usr/bin/env python3
"""
Generate dummy hashtag batch data for testing stable hashtag ranking.
Creates hashtags/batch/YYYY-MM-DD_HH:MM.json files in S3 for past 30 days.
"""

import json
import boto3
import os
from datetime import datetime, timedelta, timezone
import random

# Japan Standard Time (UTC+9)
JST = timezone(timedelta(hours=9))

# AWS S3 client
s3_client = boto3.client("s3")

# Configuration
BUCKET_NAME = os.environ.get("STATISTICS_BUCKET", "bluesky-feed-dashboard-878311109818")
REGION = "ap-northeast-1"

# Stable hashtag pool (simulating real Japanese Bluesky communities)
STABLE_HASHTAGS = {
    "おはようvtuber": (80, 150),      # High frequency, stable
    "イラスト": (60, 120),            # High frequency, stable
    "プログラミング": (40, 80),       # Medium frequency, stable
    "ゲーム": (50, 100),              # Medium frequency, stable
    "アニメ": (45, 90),               # Medium frequency, stable
    "小説": (30, 60),                 # Lower frequency, stable
    "音楽": (35, 70),                 # Lower frequency, stable
    "写真": (25, 50),                 # Lower frequency, stable
    "デザイン": (20, 45),             # Lower frequency, stable
    "technology": (15, 35),           # English tag, occasional
}

def generate_daily_batches(date):
    """
    Generate 144 batch files for a single day (one per 10 minutes).
    Each batch contains a subset of hashtags with random counts.
    """
    batches = []

    # 144 batches per day (24 * 60 / 10)
    for hour in range(24):
        for minute_block in range(0, 60, 10):
            timestamp = f"{date.strftime('%Y-%m-%d')}_{hour:02d}:{minute_block:02d}"

            # Randomly select which tags appear in this batch
            batch_hashtags = {}
            for tag, (min_count, max_count) in STABLE_HASHTAGS.items():
                # 70% chance a tag appears in any given batch
                if random.random() < 0.7:
                    count = random.randint(min_count // 10, max_count // 10)  # Smaller batches
                    batch_hashtags[tag] = count

            batches.append({
                "timestamp": timestamp,
                "data": batch_hashtags
            })

    return batches

def upload_to_s3(bucket, key, data):
    """Upload JSON data to S3"""
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"✓ Uploaded: s3://{bucket}/{key}")
    except Exception as e:
        print(f"✗ Failed to upload {key}: {e}")

def main():
    """Generate and upload dummy data for past 30 days"""

    print(f"Generating dummy hashtag batch data for past 30 days...")
    print(f"Target bucket: {BUCKET_NAME}")
    print(f"Region: {REGION}\n")

    # Get current date in JST
    now_jst = datetime.now(JST)

    # Generate data for past 30 days
    for days_ago in range(30, -1, -1):
        date = now_jst - timedelta(days=days_ago)
        date_str = date.strftime("%Y-%m-%d")

        print(f"Generating batches for {date_str}...")
        batches = generate_daily_batches(date)

        # Upload each batch
        for batch in batches:
            timestamp = batch["timestamp"]
            data = batch["data"]

            key = f"hashtags/batch/{timestamp}.json"
            upload_to_s3(BUCKET_NAME, key, data)

        print(f"  → {len(batches)} batches uploaded for {date_str}\n")

    print("Done! Dummy data generation complete.")
    print(f"Total files created: {31 * 144} (31 days × 144 batches/day)")

if __name__ == "__main__":
    main()
