"""Ingest Lambda for the async metadata pipeline."""

from __future__ import annotations

import json
import logging
import os
import urllib.parse
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

SQS = boto3.client("sqs")
S3 = boto3.client("s3")

QUEUE_URL = os.environ["QUEUE_URL"]
VALID_EXTENSIONS = (".jpg", ".jpeg", ".png")
EXPECTED_PREFIX = "incoming/"


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for the Lambda triggered by S3 notifications."""
    records = event.get("Records", [])
    sent = 0

    for record in records:
        if handle_record(record, context):
            sent += 1

    return {"records": len(records), "sent": sent}


def handle_record(record: Dict[str, Any], context: Any) -> bool:
    """Validate the S3 record and enqueue a metadata task."""
    bucket = record.get("s3", {}).get("bucket", {}).get("name")
    raw_key = record.get("s3", {}).get("object", {}).get("key")
    if not bucket or not raw_key:
        LOGGER.warning("malformed record: %s", record)
        return False

    key = urllib.parse.unquote_plus(raw_key)
    normalized = key.lower()
    if not normalized.startswith(EXPECTED_PREFIX):
        LOGGER.info("Skipping key outside prefix: %s", key)
        return False
    if not normalized.endswith(VALID_EXTENSIONS):
        LOGGER.info("Skipping non-image key: %s", key)
        return False

    etag = record.get("s3", {}).get("object", {}).get("eTag")
    etag = etag.strip('"') if isinstance(etag, str) else None
    if not etag:
        etag = fetch_object_etag(bucket, key)

    payload = {
        "bucket": bucket,
        "key": key,
        "etag": etag,
        "event_time": record.get("eventTime"),
        "request_id": _resolve_request_id(record, context),
    }

    SQS.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(payload))
    LOGGER.info("Enqueued metadata request for %s/%s", bucket, key)
    return True


def fetch_object_etag(bucket: str, key: str) -> str | None:
    """Attempt to fetch the object's ETag for downstream validation."""
    try:
        response = S3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error = exc.response.get("Error", {}).get("Code")
        if error in {"404", "NotFound", "NoSuchKey"}:
            LOGGER.warning("Object vanished before head_object %s/%s", bucket, key)
            return None
        LOGGER.warning("head_object failed for %s/%s: %s", bucket, key, exc)
        return None
    etag = response.get("ETag")
    return etag.strip('"') if isinstance(etag, str) else etag


def _resolve_request_id(record: Dict[str, Any], context: Any) -> str | None:
    response_elements = record.get("responseElements") or {}
    request_id = response_elements.get("x-amz-request-id")
    if request_id:
        return request_id
    return getattr(context, "aws_request_id", None)
