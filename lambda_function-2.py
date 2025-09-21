import os, json, boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.getenv("SHELTERS_TBL", "Shelters")
table = dynamodb.Table(TABLE_NAME)

def _scan_all(**kwargs):
    """DynamoDB scan with pagination."""
    items = []
    resp = table.scan(**kwargs)
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        items.extend(resp.get("Items", []))
    return items

def lambda_handler(event, context):
    try:
        # Optional: limit attributes you return
        items = _scan_all(ProjectionExpression="#id, lat, lng, #nm",
                          ExpressionAttributeNames={"#id":"shelter_id","#nm":"name"})

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",             # tighten in prod
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps(items)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)})
        }


dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.getenv("SHELTERS_TBL", "Shelters")
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    try:
        resp = table.scan()
        items = resp.get("Items", [])

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",  # allow frontend
                "Access-Control-Allow-Methods": "GET,OPTIONS"
            },
            "body": json.dumps(items)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)})
        }

from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.getenv("SHELTERS_TBL", "Shelters")
table = dynamodb.Table(TABLE_NAME)

def _to_jsonable(obj):
    if isinstance(obj, list):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        # keep integers as int; others as float
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

def lambda_handler(event, context):
    try:
        resp = table.scan(
            ProjectionExpression="#id, lat, lng, #nm",
            ExpressionAttributeNames={"#id": "shelter_id", "#nm": "name"},
        )
        items = _to_jsonable(resp.get("Items", []))

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Content-Type": "application/json",
            },
            "body": json.dumps(items)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)})
        }