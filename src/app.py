import json
import os
import time
import psycopg

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS registrations (
  username VARCHAR(64) PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

def _connect():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=5,
    )

def init_schema_handler(event, context):
    # RDS can be "available" but not ready to accept connections yet.
    for _ in range(12):
        try:
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(SCHEMA_SQL)
            return {"PhysicalResourceId": "InitSchema", "Data": {"status": "ok"}}
        except Exception:
            time.sleep(10)
    raise RuntimeError("DB not ready for schema init")

def register_handler(event, context):
    username = json.loads(event["body"])["username"]
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO registrations (username) VALUES (%s);", (username,))
        return {"statusCode": 201, "body": json.dumps({"username": username, "status": "reserved"})}
    except Exception:
        return {"statusCode": 409, "body": json.dumps({"username": username, "status": "taken"})}
