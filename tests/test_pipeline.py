"""End-to-end test validating the async metadata pipeline."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict

import boto3
import pytest
from botocore.exceptions import ClientError

STACK_NAME = os.getenv("STACK_NAME", "lab2-async-metadata")
TEST_IMAGE_KEY = os.getenv("TEST_IMAGE_KEY", "incoming/tiny.jpg")
POLL_TIMEOUT = int(os.getenv("POLL_TIMEOUT", "180"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))

cf = boto3.client("cloudformation")
s3 = boto3.client("s3")


def get_stack_output(stack_name: str, output_key: str) -> str:
    resp = cf.describe_stacks(StackName=stack_name)
    stacks = resp.get("Stacks", [])
    if not stacks:
        raise RuntimeError(f"Stack {stack_name} not found")
    outputs = stacks[0].get("Outputs", [])
    for output in outputs:
        if output.get("OutputKey") == output_key:
            return output["OutputValue"]
    raise KeyError(f"Output {output_key} not found on stack {stack_name}")


def upload_file_to_s3(bucket: str, key: str, path: Path) -> None:
    s3.upload_file(str(path), bucket, key)


def wait_for_s3_key(bucket: str, key: str, timeout_seconds: int = POLL_TIMEOUT) -> Dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            return s3.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"404", "NotFound", "NoSuchKey"}:
                time.sleep(POLL_INTERVAL)
                continue
            raise
    raise TimeoutError(f"Timed out waiting for s3://{bucket}/{key}")


def read_json_from_s3(bucket: str, key: str) -> Dict[str, Any]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read()
    obj["Body"].close()
    return json.loads(content)


@pytest.mark.aws_integration
def test_pipeline_processes_images_and_is_idempotent() -> None:
    bucket = get_stack_output(STACK_NAME, "BucketName")
    image_path = Path(__file__).parent / "assets" / "tiny.jpg"
    assert image_path.exists(), "Missing test asset tiny.jpg"

    upload_file_to_s3(bucket, TEST_IMAGE_KEY, image_path)
    metadata_key = build_metadata_key(TEST_IMAGE_KEY)

    head = wait_for_s3_key(bucket, metadata_key)
    data = read_json_from_s3(bucket, metadata_key)

    assert data["source_bucket"] == bucket
    assert data["source_key"] == TEST_IMAGE_KEY
    assert isinstance(data["width"], int) and data["width"] > 0
    assert isinstance(data["height"], int) and data["height"] > 0
    assert isinstance(data.get("file_size_bytes"), int) and data["file_size_bytes"] > 0
    assert isinstance(data.get("format"), str)

    original_last_modified = head["LastModified"]
    upload_file_to_s3(bucket, TEST_IMAGE_KEY, image_path)
    # ensure the queue event had a chance to be processed
    time.sleep(10)
    head_after = wait_for_s3_key(bucket, metadata_key, timeout_seconds=60)
    assert head_after["LastModified"] == original_last_modified


def build_metadata_key(source_key: str) -> str:
    filename = Path(source_key).name
    return f"metadata/{filename}.json"
