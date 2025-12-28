import io
import json
import os
import time
from typing import Any, Dict, List

import boto3
import torch
from PIL import Image
from transformers import AutoImageProcessor, AutoModelForImageClassification

s3 = boto3.client("s3")

INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "incoming/")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "predictions/")
TOP_K = int(os.environ.get("TOP_K", "3"))
HF_MODEL_ID = os.environ.get("HF_MODEL_ID", "google/vit-base-patch16-224")
MODEL_CACHE_DIR = os.environ.get("MODEL_CACHE_DIR", "/tmp/hf-model")

_processor = None
_model = None
_model_device = torch.device("cpu")

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

def _ensure_model_loaded() -> None:
    global _processor, _model
    if _processor is not None and _model is not None:
        return

    os.makedirs(MODEL_CACHE_DIR, exist_ok=True)
    _processor = AutoImageProcessor.from_pretrained(HF_MODEL_ID, cache_dir=MODEL_CACHE_DIR)
    _model = AutoModelForImageClassification.from_pretrained(HF_MODEL_ID, cache_dir=MODEL_CACHE_DIR)
    _model.eval()


def _local_classify(image_bytes: bytes, top_k: int) -> List[Dict[str, Any]]:
    _ensure_model_loaded()

    with Image.open(io.BytesIO(image_bytes)) as img:
        if img.mode != "RGB":
            img = img.convert("RGB")
        inputs = _processor(images=img, return_tensors="pt")

    inputs = {k: v.to(_model_device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = _model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1)[0]

    k = max(1, min(top_k, probs.shape[-1]))
    values, indices = torch.topk(probs, k)

    id2label = getattr(_model.config, "id2label", {}) or {}
    results = []
    for score, idx in zip(values.tolist(), indices.tolist()):
        label = id2label.get(idx, str(idx))
        results.append({"label": label, "score": float(score)})
    return results

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
        preds = _local_classify(image_bytes=image_bytes, top_k=TOP_K)
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
