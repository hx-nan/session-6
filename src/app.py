import json
import os
import boto3

rds = boto3.client("rds-data")

DB_ARN = os.environ["DB_ARN"]
SECRET_ARN = os.environ["SECRET_ARN"]
DB_NAME = os.environ["DB_NAME"]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS registrations (
  username VARCHAR(64) PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def init_schema_handler(event, context):
    rds.execute_statement(
        resourceArn=DB_ARN,
        secretArn=SECRET_ARN,
        database=DB_NAME,
        sql=SCHEMA_SQL,
    )
    return {"PhysicalResourceId": "InitSchema", "Data": {"status": "ok"}}


def register_handler(event, context):
    body = json.loads(event["body"])
    username = body["username"]

    try:
        rds.execute_statement(
            resourceArn=DB_ARN,
            secretArn=SECRET_ARN,
            database=DB_NAME,
            sql="INSERT INTO registrations (username) VALUES (:u);",
            parameters=[{"name": "u", "value": {"stringValue": username}}],
        )
        return {"statusCode": 201, "body": json.dumps({"username": username, "status": "reserved"})}
    except Exception:
        return {"statusCode": 409, "body": json.dumps({"username": username, "status": "taken"})}
