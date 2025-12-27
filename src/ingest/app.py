import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

QUEUE_URL = os.environ["QUEUE_URL"]
INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "incoming/")
ALLOWED_SUFFIXES = tuple(
    s.strip().lower()
    for s in os.environ.get("ALLOWED_SUFFIXES", ".png,.jpg,.jpeg").split(",")
    if s.strip()
)

def _is_allowed_key(key: str) -> bool:
    k = key.lower()
    return k.startswith(INPUT_PREFIX) and k.endswith(ALLOWED_SUFFIXES)

def lambda_handler(event, context):
    records = event.get("Records", [])
    enqueued = 0
    skipped = 0

    for r in records:
        if r.get("eventSource") != "aws:s3":
            skipped += 1
            continue

        bucket = r["s3"]["bucket"]["name"]
        # S3 keys are URL-encoded in events
        key = urllib.parse.unquote_plus(r["s3"]["object"]["key"])

        if not _is_allowed_key(key):
            skipped += 1
            continue

        # Get ETag (useful for idempotency + traceability)
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            etag = (head.get("ETag") or "").strip('"')
        except Exception:
            # If head fails, still enqueue; inference lambda can proceed or fail/retry.
            etag = ""

        msg = {
            "bucket": bucket,
            "key": key,
            "etag": etag,
            "event_time": datetime.now(timezone.utc).isoformat(),
            "request_id": getattr(context, "aws_request_id", ""),
        }

        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(msg),
        )
        enqueued += 1

    return {
        "statusCode": 200,
        "enqueued": enqueued,
        "skipped": skipped,
    }
