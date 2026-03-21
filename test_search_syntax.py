#!/usr/bin/env python3
"""
Test script to verify Bluesky search_posts query syntax support
Testing OR operator and hashtag combinations
"""

import os
from dotenv import load_dotenv
from atproto_client import Client
import json

load_dotenv()

BSKY_HANDLE = os.environ.get("BSKY_HANDLE")
BSKY_APP_PASSWORD = os.environ.get("BSKY_APP_PASSWORD")

def test_search_queries():
    """Test various search query syntaxes"""

    client = Client()
    profile = client.login(BSKY_HANDLE, BSKY_APP_PASSWORD)
    print(f"✓ Logged in as {profile.handle}")

    # Test queries
    test_cases = [
        {
            "name": "Basic lang filter",
            "query": "lang:ja",
            "expected": "Should work (current implementation)"
        },
        {
            "name": "Hashtag only",
            "query": "#vtuber",
            "expected": "Testing single hashtag"
        },
        {
            "name": "Lang + single hashtag (space)",
            "query": "lang:ja #vtuber",
            "expected": "Testing AND with space separator"
        },
        {
            "name": "Lang + multiple hashtags (space)",
            "query": "lang:ja #vtuber #anime",
            "expected": "Testing multiple hashtags"
        },
        {
            "name": "Multiple hashtags with OR",
            "query": "lang:ja (#vtuber OR #anime)",
            "expected": "Testing OR with parentheses"
        },
        {
            "name": "Multiple hashtags with pipe",
            "query": "lang:ja #vtuber | #anime",
            "expected": "Testing OR with pipe operator"
        },
    ]

    results = []

    for test in test_cases:
        print(f"\n{'='*60}")
        print(f"Test: {test['name']}")
        print(f"Query: {test['query']}")
        print(f"Expected: {test['expected']}")
        print(f"{'='*60}")

        try:
            res = client.app.bsky.feed.search_posts({
                "q": test['query'],
                "sort": "latest",
                "limit": 10,
            })

            result = {
                "name": test['name'],
                "query": test['query'],
                "status": "✓ SUCCESS",
                "posts_found": len(res.posts),
                "error": None
            }

            print(f"✓ SUCCESS - Found {len(res.posts)} posts")
            if res.posts:
                print(f"  Sample post: {res.posts[0].record.text[:100]}...")

        except Exception as e:
            result = {
                "name": test['name'],
                "query": test['query'],
                "status": "✗ FAILED",
                "posts_found": 0,
                "error": str(e)
            }
            print(f"✗ FAILED - {str(e)[:100]}")

        results.append(result)

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")

    for result in results:
        status = result['status']
        print(f"{status}: {result['name']}")
        print(f"  Query: {result['query']}")
        if result['error']:
            print(f"  Error: {result['error'][:80]}")
        else:
            print(f"  Posts: {result['posts_found']}")

    # Save results
    with open('/tmp/search_test_results.json', 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResults saved to /tmp/search_test_results.json")

if __name__ == '__main__':
    test_search_queries()
