#!/usr/bin/env python3
"""
Test script to update dashboard.json with stable hashtags.
"""

import json
import boto3
import os
from datetime import datetime, timedelta, timezone

# Japan Standard Time (UTC+9)
JST = timezone(timedelta(hours=9))

# AWS S3 client
s3_client = boto3.client("s3")

# Configuration
BUCKET_NAME = os.environ.get("STATISTICS_BUCKET", "bluesky-feed-dashboard-878311109818")
REGION = "ap-northeast-1"


def extract_stable_hashtags(bucket, days=30, top_n=10):
    """Extract TOP stable hashtags from past N days of daily files."""
    try:
        now = datetime.now(JST)
        aggregated_all = {}

        print(f"Extracting stable hashtags from past {days} days...")

        for days_ago in range(days, -1, -1):
            date = now - timedelta(days=days_ago)
            date_str = date.strftime("%Y-%m-%d")
            daily_key = f"hashtags/daily/{date_str}.json"

            try:
                response = s3_client.get_object(Bucket=bucket, Key=daily_key)
                daily_data = json.loads(response["Body"].read().decode("utf-8"))

                for tag, count in daily_data.items():
                    aggregated_all[tag] = aggregated_all.get(tag, 0) + count
            except Exception as e:
                pass

        sorted_hashtags = sorted(
            aggregated_all.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]

        result = [
            {"tag": tag, "count": count}
            for tag, count in sorted_hashtags
        ]

        print(f"Extracted {len(result)} stable hashtags")
        return result
    except Exception as e:
        print(f"ERROR: Failed to extract stable hashtags: {str(e)}")
        return []


def update_dashboard_json(bucket, stable_hashtags):
    """Update dashboard.json with stable_hashtags in latest section."""
    try:
        # Read current dashboard.json
        dashboard_key = "stats/summary/dashboard.json"
        response = s3_client.get_object(Bucket=bucket, Key=dashboard_key)
        dashboard_data = json.loads(response["Body"].read().decode("utf-8"))

        print(f"Current latest keys: {list(dashboard_data['latest'].keys())}")

        # Remove only top_hashtags (ALL), keep top_hashtags_1h (1H trend)
        if "top_hashtags" in dashboard_data["latest"]:
            del dashboard_data["latest"]["top_hashtags"]

        # Add stable_hashtags
        dashboard_data["latest"]["stable_hashtags"] = stable_hashtags
        dashboard_data["generated_at"] = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

        print(f"Updated latest keys: {list(dashboard_data['latest'].keys())}")

        # Save updated dashboard.json
        s3_client.put_object(
            Bucket=bucket,
            Key=dashboard_key,
            Body=json.dumps(dashboard_data, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )

        print(f"✓ Updated dashboard.json with stable_hashtags")
    except Exception as e:
        print(f"ERROR: Failed to update dashboard: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    print(f"Bucket: {BUCKET_NAME}\n")

    # Extract stable hashtags
    stable_hashtags = extract_stable_hashtags(BUCKET_NAME, days=30, top_n=10)

    if stable_hashtags:
        print("\nStable Hashtags extracted:")
        for i, item in enumerate(stable_hashtags, 1):
            print(f"  {i}. #{item['tag']}: {item['count']}")

        # Update dashboard.json
        update_dashboard_json(BUCKET_NAME, stable_hashtags)
        print("\nDone!")
    else:
        print("\nNo hashtags to update.")


if __name__ == "__main__":
    main()
