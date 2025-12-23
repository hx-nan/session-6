import os
import io
import urllib.parse

import boto3
from PIL import Image, ImageOps

s3 = boto3.client("s3")

INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "incoming/")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "grayscale/")


def lambda_handler(event, context):
    # S3 Put event
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    # Guard 1: never re-process outputs
    if key.startswith(OUTPUT_PREFIX):
        return {"status": "skipped", "reason": "already output prefix", "key": key}

    # Guard 2: only handle intended input prefix (extra safety beyond S3 filter)
    if not key.startswith(INPUT_PREFIX):
        return {"status": "skipped", "reason": "not input prefix", "key": key}

    # Basic extension check (optional but helpful)
    lower = key.lower()
    if not (lower.endswith(".png") or lower.endswith(".jpg") or lower.endswith(".jpeg")):
        return {"status": "skipped", "reason": "not an image extension", "key": key}

    # Download image
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()

    # Convert to grayscale
    img = Image.open(io.BytesIO(data))
    img = ImageOps.grayscale(img)

    # Preserve extension; choose an output key under OUTPUT_PREFIX
    filename = key.split("/")[-1]
    out_key = f"{OUTPUT_PREFIX}{filename}"

    # Encode and upload
    out_buf = io.BytesIO()
    # If PNG keep PNG; otherwise save as JPEG
    if lower.endswith(".png"):
        img.save(out_buf, format="PNG")
        content_type = "image/png"
    else:
        img.save(out_buf, format="JPEG", quality=90)
        content_type = "image/jpeg"

    out_buf.seek(0)
    s3.put_object(
        Bucket=bucket,
        Key=out_key,
        Body=out_buf.getvalue(),
        ContentType=content_type,
    )

    return {"status": "ok", "input": key, "output": out_key}
