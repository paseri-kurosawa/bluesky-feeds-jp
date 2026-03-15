#!/bin/bash
# Update stats-index.json in S3 with latest stat files
# Run this periodically (e.g., every hour via cron or Lambda)

BUCKET="bluesky-feed-statistics-878311109818"
INDEX_FILE="/tmp/stats-index-$(date +%s).json"

echo "Generating stats index..."
# List both .md and .json files
aws s3 ls s3://$BUCKET/stats/ --recursive | grep -E '\.(md|json)$' | sort -k1,2 | tail -200 | awk '{print $4}' | jq -R -s 'split("\n") | map(select(length > 0))' > "$INDEX_FILE"

echo "Uploading index to S3..."
aws s3 cp "$INDEX_FILE" s3://$BUCKET/stats-index.json \
  --content-type application/json \
  --cache-control "max-age=60" \
  --metadata "auto-generated=true,updated=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "Done! Index updated at $(date)"
rm -f "$INDEX_FILE"
