#!/usr/bin/env python3
"""
Test script to extract stable hashtags from past 30 days of dummy data.
Generates hashtags/ranking/stable_hashtags.json in S3.
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


def aggregate_batch_files_for_date(bucket, target_date):
    """
    Aggregate all batch files for a specific date.

    Args:
        bucket: S3 bucket name
        target_date: Date string in format YYYY-MM-DD

    Returns:
        Dict of aggregated hashtags {tag: total_count}
    """
    try:
        # List all batch files for this date
        prefix = f"hashtags/batch/{target_date}_"
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        aggregated = {}
        batch_count = 0
        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                response = s3_client.get_object(Bucket=bucket, Key=key)
                batch_data = json.loads(response["Body"].read().decode("utf-8"))

                # Aggregate hashtags
                for tag, count in batch_data.items():
                    aggregated[tag] = aggregated.get(tag, 0) + count

                batch_count += 1

        return aggregated if aggregated else None, batch_count
    except Exception as e:
        print(f"ERROR: Failed to aggregate batch files for {target_date}: {str(e)}")
        return None, 0


def extract_stable_hashtags(bucket, days=30, top_n=10):
    """
    Extract TOP stable hashtags from past N days by aggregating batch files.

    Args:
        bucket: S3 bucket name
        days: Number of days to analyze (default 30)
        top_n: Number of top hashtags to return (default 10)

    Returns:
        List of dicts [{"tag": "...", "count": ...}, ...]
    """
    try:
        now = datetime.now(JST)
        aggregated_all = {}

        print(f"Extracting stable hashtags from past {days} days...")
        print(f"Current time (JST): {now.strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Read and aggregate batch files for past N days
        for days_ago in range(days, -1, -1):
            date = now - timedelta(days=days_ago)
            date_str = date.strftime("%Y-%m-%d")

            daily_aggregated, batch_count = aggregate_batch_files_for_date(bucket, date_str)

            if daily_aggregated:
                # Aggregate all hashtags
                for tag, count in daily_aggregated.items():
                    aggregated_all[tag] = aggregated_all.get(tag, 0) + count

                print(f"  ✓ {date_str}: {len(daily_aggregated)} unique tags from {batch_count} batches")
            else:
                print(f"  ✗ {date_str}: No batch files found")

        print(f"\nTotal unique hashtags across {days} days: {len(aggregated_all)}")
        print(f"Extracting TOP {top_n}...\n")

        # Sort by count descending and get top N
        sorted_hashtags = sorted(
            aggregated_all.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]

        result = [
            {"tag": tag, "count": count}
            for tag, count in sorted_hashtags
        ]

        # Display results
        print("TOP Stable Hashtags:")
        for i, item in enumerate(result, 1):
            print(f"  {i}. #{item['tag']}: {item['count']} posts")

        return result
    except Exception as e:
        print(f"ERROR: Failed to extract stable hashtags: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


def save_stable_hashtags_ranking(bucket, stable_hashtags):
    """
    Save stable hashtags ranking to S3.

    Args:
        bucket: S3 bucket name
        stable_hashtags: List of {"tag": ..., "count": ...}
    """
    try:
        now = datetime.now(JST)
        ranking_data = {
            "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "analysis_period_days": 30,
            "top_hashtags": stable_hashtags
        }

        s3_key = "hashtags/summary/stable_hashtags.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(ranking_data, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"\n✓ Saved summary: s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"\nERROR: Failed to save summary: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Region: {REGION}\n")

    # Extract stable hashtags
    stable_hashtags = extract_stable_hashtags(BUCKET_NAME, days=30, top_n=10)

    # Save to S3
    if stable_hashtags:
        save_stable_hashtags_ranking(BUCKET_NAME, stable_hashtags)
        print("\nDone!")
    else:
        print("\nNo hashtags to save.")


if __name__ == "__main__":
    main()
