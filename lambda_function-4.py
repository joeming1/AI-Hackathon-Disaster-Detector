# Lambda: API_ALERTS_GET  (Python 3.11+)
# Env vars:
#   ALERTS_TBL=DisasterAlerts

import os, json, boto3
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.getenv("ALERTS_TBL", "DisasterAlerts")
table = dynamodb.Table(TABLE_NAME)

def _cors():
    return {
        "Access-Control-Allow-Origin": "*",           # tighten to your domain in prod
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }

def _to_jsonable(obj):
    """Recursively convert Decimal -> int/float for JSON serialization."""
    if isinstance(obj, list):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

def _scan_all(**kwargs):
    items, resp = [], table.scan(**kwargs)
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        items.extend(resp.get("Items", []))
    return items

def lambda_handler(event, context):
    # CORS preflight
    method = (event.get("httpMethod")
              or event.get("requestContext", {}).get("http", {}).get("method"))
    if method == "OPTIONS":
        return {"statusCode": 204, "headers": _cors(), "body": ""}

    try:
        # Return selected fields; adjust as needed
        items = _scan_all(
            ProjectionExpression="#id, confidence, location, polygon, status, #ts, tweet_text",
            ExpressionAttributeNames={"#id": "alert_id", "#ts": "timestamp"},
        )

        payload = _to_jsonable(items)

        return {
            "statusCode": 200,
            "headers": _cors(),
            "body": json.dumps(payload),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": _cors(),
            "body": json.dumps({"error": str(e)}),
        }

