import json
import os

def lambda_handler(event, context):
    """
    DID Document endpoint: /.well-known/did.json
    Returns AT Protocol compliant DID document.
    """
    feed_did = os.environ.get('FEED_DID', 'did:web:example.com')
    service_endpoint = os.environ.get('SERVICE_ENDPOINT', 'https://example.com')

    did_document = {
        "@context": "https://w3id.org/did/v1",
        "id": feed_did,
        "service": [
            {
                "id": "#bsky_fg",
                "type": "BskyFeedGenerator",
                "serviceEndpoint": service_endpoint
            }
        ]
    }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(did_document)
    }
