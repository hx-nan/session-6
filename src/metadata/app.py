"""Metadata Lambda that renders image metadata to JSON."""

from __future__ import annotations

import json
import logging
import os
from io import BytesIO
from pathlib import Path
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError
from PIL import Image, ExifTags

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

S3 = boto3.client("s3")

METADATA_PREFIX = os.environ.get("METADATA_PREFIX", "metadata/")


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """Handle SQS batch events."""
    processed = 0
    for record in event.get("Records", []):
        if handle_message(record):
            processed += 1
    return {"processed": processed}


def handle_message(record: Dict[str, Any]) -> bool:
    """Process a single SQS message and persist metadata JSON."""
    body = record.get("body")
    if not body:
        LOGGER.warning("Missing body on record: %s", record)
        return False

    payload = json.loads(body)
    bucket = payload["bucket"]
    key = payload["key"]
    out_key = build_output_key(key)

    if metadata_exists(bucket, out_key):
        LOGGER.info("Metadata already exists for %s/%s", bucket, key)
        return True

    response = S3.get_object(Bucket=bucket, Key=key)
    try:
        content = response["Body"].read()
    finally:
        response["Body"].close()

    metadata = extract_metadata(content, response, payload, out_key)
    S3.put_object(
        Bucket=bucket,
        Key=out_key,
        Body=json.dumps(metadata, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    LOGGER.info("Wrote metadata object %s/%s", bucket, out_key)
    return True


def build_output_key(source_key: str) -> str:
    filename = Path(source_key).name
    return f"{METADATA_PREFIX.rstrip('/')}/{filename}.json"


def metadata_exists(bucket: str, key: str) -> bool:
    try:
        S3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NotFound", "NoSuchKey"}:
            return False
        raise


def extract_metadata(content: bytes, response: Dict[str, Any], payload: Dict[str, Any], out_key: str) -> Dict[str, Any]:
    """Extract image metadata by loading via Pillow."""
    with Image.open(BytesIO(content)) as image:
        width, height = image.size
        image_metadata = {
            "source_bucket": payload["bucket"],
            "source_key": payload["key"],
            "destination_key": out_key,
            "width": width,
            "height": height,
            "mode": image.mode,
            "format": image.format,
            "file_size_bytes": response.get("ContentLength"),
            "etag": payload.get("etag"),
            "event_time": payload.get("event_time"),
            "request_id": payload.get("request_id"),
        }
        exif = read_exif(image)
        if exif:
            image_metadata["exif"] = exif
        return image_metadata


def read_exif(image: Image.Image) -> Dict[str, Any]:
    """Read EXIF values and convert them into strings."""
    exif_data = {}
    try:
        raw_exif = image.getexif()
    except (AttributeError, ValueError, OSError):
        return exif_data
    if not raw_exif:
        return exif_data

    for tag_id, value in raw_exif.items():
        tag_name = ExifTags.TAGS.get(tag_id, str(tag_id))
        exif_data[tag_name] = stringify(value)
    return exif_data


def stringify(value: Any) -> Any:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:  # pragma: no cover - defensive
            return str(value)
    if isinstance(value, (list, tuple)):
        return [stringify(v) for v in value]
    return value
