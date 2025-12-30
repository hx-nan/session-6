import json
import os
import time
import secrets
import string

import boto3

TABLE_NAME = os.environ["TABLE_NAME"]
CODE_LENGTH = 7

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

ALPHABET = string.ascii_letters + string.digits


def _new_code():
    return "".join(secrets.choice(ALPHABET) for _ in range(CODE_LENGTH))


def create_handler(event, context):
    """
    POST /shorten
    Body: {"url": "https://example.com"}
    """
    body = json.loads(event["body"])
    long_url = body["url"]

    code = _new_code()
    now = int(time.time())

    table.put_item(
        Item={
            "code": code,
            "long_url": long_url,
            "created_at": now,
            "expires_at": now + 7 * 24 * 60 * 60,  # 7 days TTL
        },
        ConditionExpression="attribute_not_exists(code)",
    )

    rc = event["requestContext"]
    short_url = f"https://{rc['domainName']}/{rc['stage']}/{code}"

    return {
        "statusCode": 201,
        "body": json.dumps({"code": code, "short_url": short_url}),
    }


def resolve_handler(event, context):
    """
    GET /{code}
    """
    code = event["pathParameters"]["code"]

    resp = table.get_item(
        Key={"code": code},
        ConsistentRead=True,
    )

    return {
        "statusCode": 302,
        "headers": {"Location": resp["Item"]["long_url"]},
        "body": "",
    }
