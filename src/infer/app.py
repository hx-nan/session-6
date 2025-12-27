import json
import os
import time
import urllib.request
import urllib.error
from typing import Any, Dict, List

import boto3

s3 = boto3.client("s3")

INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "incoming/")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "predictions/")
TOP_K = int(os.environ.get("TOP_K", "3"))
HF_MODEL_ID = os.environ.get("HF_MODEL_ID", "google/vit-base-patch16-224")
HF_TOKEN = os.environ.get("HF_TOKEN", "").strip()

HF_URL = f"https://api-inference.huggingface.co/models/{HF_MODEL_ID}"

def _prediction_key_for(source_key: str) -> str:
    # Keep the original filename but append .json
    # incoming/cat.jpg -> predictions/cat.jpg.json
    if source_key.startswith(INPUT_PREFIX):
        tail = source_key[len(INPUT_PREFIX):]
    else:
        tail = source_key
    return f"{OUTPUT_PREFIX}{tail}.json"

def _head_object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def _hf_classify(image_bytes: bytes, top_k: int) -> List[Dict[str, Any]]:
    headers = {
        "Content-Type": "application/octet-stream",
    }
    if HF_TOKEN:
        headers["Authorization"] = f"Bearer {HF_TOKEN}"

    # Ask for top_k results if supported; the Inference API accepts parameters for many pipelines.
    payload = {
        "inputs": None,  # we send raw bytes, so inputs is implicit
        "parameters": {"top_k": top_k},
        "options": {"wait_for_model": True},
    }

    # The HF Inference API supports raw bytes directly.
    # However, passing JSON + bytes together isn't standard. The simplest reliable approach:
    # Send image bytes only, and accept default top_k behavior; then slice top_k in code.
    # (Some models accept params, but consistency varies.)
    req = urllib.request.Request(HF_URL, data=image_bytes, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            body = resp.read()
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HF HTTPError {e.code}: {err_body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"HF URLError: {str(e)}") from e

    decoded = json.loads(body.decode("utf-8"))

    # Typical response for image classification: list[{"label": "...", "score": 0.99}, ...]
    if isinstance(decoded, dict) and decoded.get("error"):
        raise RuntimeError(f"HF error: {decoded.get('error')}")

    if not isinstance(decoded, list):
        raise RuntimeError(f"Unexpected HF response shape: {type(decoded)}")

    return decoded[:top_k]

def lambda_handler(event, context):
    records = event.get("Records", [])
    results = []

    for r in records:
        msg = json.loads(r["body"])
        bucket = msg["bucket"]
        key = msg["key"]
        etag = msg.get("etag", "")

        if not key.startswith(INPUT_PREFIX):
            # Not our concern; skip safely.
            results.append({"key": key, "status": "skipped_non_input_prefix"})
            continue

        out_key = _prediction_key_for(key)

        # Idempotency: if output exists, skip re-inference
        if _head_object_exists(bucket, out_key):
            results.append({"key": key, "status": "skipped_already_processed", "out_key": out_key})
            continue

        # Download image
        obj = s3.get_object(Bucket=bucket, Key=key)
        image_bytes = obj["Body"].read()

        t0 = time.time()
        preds = _hf_classify(image_bytes=image_bytes, top_k=TOP_K)
        inference_ms = int((time.time() - t0) * 1000)

        output_doc = {
            "source_bucket": bucket,
            "source_key": key,
            "source_etag": etag,
            "model_id": HF_MODEL_ID,
            "top_k": TOP_K,
            "predictions": preds,
            "inference_ms": inference_ms,
            "request_id": getattr(context, "aws_request_id", ""),
        }

        s3.put_object(
            Bucket=bucket,
            Key=out_key,
            Body=json.dumps(output_doc, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        results.append({"key": key, "status": "ok", "out_key": out_key, "inference_ms": inference_ms})

    return {"statusCode": 200, "results": results}
