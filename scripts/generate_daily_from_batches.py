#!/usr/bin/env python3
"""
Generate daily hashtag files from batch files.
Aggregates all batch files for each day and saves to hashtags/daily/
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
        Tuple of (aggregated_dict, batch_count)
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

        return aggregated, batch_count
    except Exception as e:
        print(f"ERROR: Failed to aggregate batch files for {target_date}: {str(e)}")
        return None, 0


def save_daily_file(bucket, target_date, aggregated):
    """
    Save daily aggregated hashtags to S3.

    Args:
        bucket: S3 bucket name
        target_date: Date string in format YYYY-MM-DD
        aggregated: Dict of aggregated hashtags
    """
    try:
        s3_key = f"hashtags/daily/{target_date}.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(aggregated, ensure_ascii=False, indent=2),
            ContentType="application/json; charset=utf-8"
        )
        print(f"  ✓ Saved: s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"  ✗ Failed to save {s3_key}: {str(e)}")


def main():
    print(f"Generating daily hashtag files from batches...")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Region: {REGION}\n")

    now = datetime.now(JST)

    # Generate daily files for past 31 days (covering all dummy data)
    for days_ago in range(31, -1, -1):
        date = now - timedelta(days=days_ago)
        date_str = date.strftime("%Y-%m-%d")

        print(f"Processing {date_str}...")

        # Aggregate batch files for this date
        aggregated, batch_count = aggregate_batch_files_for_date(BUCKET_NAME, date_str)

        if aggregated:
            save_daily_file(BUCKET_NAME, date_str, aggregated)
            print(f"  Aggregated {batch_count} batches, {len(aggregated)} unique tags")
        else:
            print(f"  No batch files found")

    print("\nDone!")


if __name__ == "__main__":
    main()
