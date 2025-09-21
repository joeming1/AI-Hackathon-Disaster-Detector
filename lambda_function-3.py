import os, re, json, math, urllib.request, urllib.parse, boto3
from datetime import datetime, timezone
import uuid

# Initialize clients
dynamodb = boto3.resource("dynamodb")
sns_client = boto3.client("sns")

# Environment variables with defaults
SHELTERS_TBL = os.getenv("SHELTERS_TBL", "Shelters")
ALERTS_TBL = os.getenv("ALERTS_TBL", "Alerts")
RESIDENTS_TBL = os.getenv("RESIDENTS_TBL", "Resident_info")
ROUTES_TABLE = os.getenv("ROUTES_TABLE", "EvacuationRoutes")
MAX_STEPS = int(os.getenv("MAX_STEPS", "5"))
GMP_API_KEY = os.getenv("GMP_API_KEY")
SNS_REGION = os.getenv("SNS_REGION", "us-east-1")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
MOCK_MODE = os.getenv("MOCK_MODE", "true").lower() == "true"

# ---------- small geo helpers ----------
def haversine_km(a, b):
    R = 6371.0088
    dlat = math.radians(b["lat"] - a["lat"])
    dlng = math.radians(b["lng"] - a["lng"])
    lat1 = math.radians(a["lat"])
    lat2 = math.radians(b["lat"])
    h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlng/2)**2
    return 2 * R * math.asin(math.sqrt(h))

def point_in_polygon(pt, ring):
    x, y = pt["lng"], pt["lat"]
    inside = False
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        if ((y1 > y) != (y2 > y)) and (x < (x2 - x1) * (y - y1) / (y2 - y1 + 1e-12) + x1):
            inside = not inside
    return inside

def parse_poly(poly_str):
    gj = json.loads(poly_str)
    return gj["coordinates"][0]  # exterior ring

def bearing_deg(a, b):
    lat1 = math.radians(a["lat"]); lat2 = math.radians(b["lat"])
    dlon = math.radians(b["lng"] - a["lng"])
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1)*math.sin(lat2) - math.sin(lat1)*math.cos(lat2)*math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0

def cardinal(deg):
    dirs = ["N","NE","E","SE","S","SW","W","NW","N"]
    return dirs[int((deg + 22.5)//45)]

def strip_html(s):
    return re.sub("<[^>]+>", "", s or "")

# ---------- Google Directions ----------
def gmaps_steps(user, dest, lang="en", max_steps=5):
    if not GMP_API_KEY:
        return None
    params = {
        "origin":      f"{user['lat']},{user['lng']}",
        "destination": f"{dest['lat']},{dest['lng']}",
        "mode":        "driving",
        "language":    lang,
        "key":         GMP_API_KEY
    }
    url = "https://maps.googleapis.com/maps/api/directions/json?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=8) as r:
        data = json.loads(r.read().decode())

    if data.get("status") != "OK":
        return None

    leg = data["routes"][0]["legs"][0]
    steps = [
        f"{strip_html(s['html_instructions'])} for {int(s['distance']['value'])} m"
        for s in leg["steps"][:max_steps]
    ]
    dist_km = round(leg["distance"]["value"]/1000, 2)
    eta_min = int(round(leg["duration"]["value"]/60))
    return steps, dist_km, eta_min

# ---------- Route Management Functions ----------
def get_precalculated_routes(alert_id, user_location):
    """Check if routes have already been calculated for this alert"""
    routes_tbl = dynamodb.Table(ROUTES_TABLE)
    
    try:
        # Query routes for this alert
        response = routes_tbl.query(
            IndexName="alert_idx",
            KeyConditionExpression=boto3.dynamodb.conditions.Key("alert_id").eq(alert_id)
        )
        
        routes = response.get("Items", [])
        
        # If no routes found, return empty list
        if not routes:
            return []
            
        # Find the closest route to the user
        closest_route = None
        min_distance = float("inf")
        
        for route in routes:
            # Calculate distance from user to route destination
            dest_coords = {"lat": float(route.get("dest_lat", 0)), "lng": float(route.get("dest_lng", 0))}
            distance = haversine_km(user_location, dest_coords)
            
            if distance < min_distance:
                min_distance = distance
                closest_route = route
        
        return [closest_route] if closest_route else []
        
    except Exception as e:
        print(f"Error querying pre-calculated routes: {e}")
        return []

def store_user_route(alert_id, user_location, shelter, steps, dist_km, eta_min):
    """Store a user-specific route in the database"""
    routes_tbl = dynamodb.Table(ROUTES_TABLE)
    
    route_id = str(uuid.uuid4())
    item = {
        "route_id": route_id,
        "alert_id": alert_id,
        "user_lat": user_location["lat"],
        "user_lng": user_location["lng"],
        "dest_lat": shelter["lat"],
        "dest_lng": shelter["lng"],
        "shelter_id": shelter["id"],
        "shelter_name": shelter["name"],
        "distance_km": dist_km,
        "eta_min": eta_min,
        "steps": steps,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
        "user_specific": True
    }
    
    try:
        routes_tbl.put_item(Item=item)
        return route_id
    except Exception as e:
        print(f"Error storing user route: {e}")
        return None

# ---------- main handler ----------
def lambda_handler(event, context):
    # Check if this is an SQS-triggered invocation
    if 'Records' in event and len(event['Records']) > 0 and 'body' in event['Records'][0]:
        # This is an SQS-triggered invocation
        return handle_sqs_event(event, context)
    else:
        # This is an API Gateway-triggered invocation
        return handle_api_request(event, context)

def handle_sqs_event(event, context):
    """
    Processes SQS events containing disaster alerts, calculates evacuation routes,
    and stores them in DynamoDB.
    """
    print(f"Received SQS event: {json.dumps(event)}")
    
    # Process each SQS record
    for record in event['Records']:
        try:
            # The SNS message is in the SQS record body
            sns_message = json.loads(record['body'])
            # The actual alert payload is in the 'Message' field of the SNS message
            alert_payload = json.loads(sns_message['Message'])
            
            print(f"Processing alert: {alert_payload['alert_id']}")
            
            # 1. Store the alert in DynamoDB (Alerts table)
            store_alert(alert_payload)
            
            # 2. Calculate routes (using mock data for demo reliability)
            routes = calculate_routes(alert_payload)
            
            # 3. Store the calculated routes
            store_routes(alert_payload['alert_id'], routes)
            
            print(f"Successfully processed alert {alert_payload['alert_id']}")
            
        except Exception as e:
            print(f"Error processing record: {e}")
            # Consider adding logic to send failed messages to a Dead-Letter Queue here

    return {'statusCode': 200, 'body': json.dumps('Processing complete.')}

def handle_api_request(event, context):
    """
    Handles API Gateway requests for evacuation routes.
    """
    # Parse body (supports both string and dict)
    body_raw = event.get("body")
    if isinstance(body_raw, str):
        try:
            body = json.loads(body_raw or "{}")
        except Exception:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "BAD_JSON"})
            }
    else:
        body = body_raw or {}

    # Accept either user coordinates or phone for lookup
    alert_id = body.get("alert_id")
    lang = body.get("lang", "en")
    phone = body.get("phone")
    require_inside = bool(body.get("require_inside", False))

    if not alert_id:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "MISSING_FIELDS", "hint": "alert_id is required"})
        }

    if "user" in body and body["user"] is not None:
        user = {"lat": float(body["user"]["lat"]), "lng": float(body["user"]["lng"])}
    elif phone:
        # Lookup user location from Resident_info table
        residents_tbl = dynamodb.Table(RESIDENTS_TBL)
        r = residents_tbl.get_item(Key={"phone": phone}).get("Item")
        if not r or "lat" not in r or "lng" not in r:
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "USER_NOT_FOUND_OR_NO_LOCATION"})
            }
        user = {"lat": float(r["lat"]), "lng": float(r["lng"])}
        lang = body.get("lang", r.get("lang", "en"))
    else:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "MISSING_FIELDS", "hint": "user or phone is required"})
        }

    alerts_tbl = dynamodb.Table(ALERTS_TBL)
    shelters_tbl = dynamodb.Table(SHELTERS_TBL)

    # Load active alert + polygon
    alert = alerts_tbl.get_item(Key={"alert_id": alert_id}).get("Item")
    if not alert or alert.get("status") != "active":
        return {
            "statusCode": 404,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "ALERT_NOT_FOUND"})
        }

    ring = parse_poly(alert["polygon"])

    # Check if user is inside the polygon if required
    user_inside = point_in_polygon(user, ring)
    if require_inside and not user_inside:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"affected": False, "reason": "USER_OUTSIDE_POLYGON"})
        }

    # Check if we have pre-calculated routes for this alert
    precalculated_routes = get_precalculated_routes(alert_id, user)
    
    if precalculated_routes:
        # Use the pre-calculated route
        route = precalculated_routes[0]
        best = {
            "id": route["shelter_id"],
            "name": route["shelter_name"],
            "lat": float(route["dest_lat"]),
            "lng": float(route["dest_lng"])
        }
        dist_km = route["distance_km"]
        eta_min = route["eta_min"]
        steps = route["steps"]
        
        # Store a user-specific version of this route
        store_user_route(alert_id, user, best, steps, dist_km, eta_min)
    else:
        # Load shelters and pick nearest outside the polygon
        items = []
        scan = shelters_tbl.scan(Limit=500)
        items.extend(scan.get("Items", []))
        while "LastEvaluatedKey" in scan:
            scan = shelters_tbl.scan(Limit=500, ExclusiveStartKey=scan["LastEvaluatedKey"])
            items.extend(scan.get("Items", []))

        candidates = []
        for it in items:
            s = {
                "id": it["shelter_id"],
                "name": it["name"],
                "lat": float(it["lat"]),
                "lng": float(it["lng"]),
            }
            if not point_in_polygon({"lat": s["lat"], "lng": s["lng"]}, ring):
                s["crow_km"] = haversine_km(user, s)
                s["bearing"] = bearing_deg(user, s)
                candidates.append(s)

        if not candidates:
            return {
                "statusCode": 409,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "NO_SAFE_SHELTERS"})
            }

        candidates.sort(key=lambda x: x["crow_km"])
        best = candidates[0]

        # Try Google Directions → else compass/bearing fallback
        steps = dist_km = eta_min = None
        try:
            result = gmaps_steps(user, best, lang=lang, max_steps=MAX_STEPS)
            if result:
                steps, dist_km, eta_min = result
        except Exception:
            steps = None  # fall through to bearing

        if not steps:
            dist_km = round(best["crow_km"], 1)
            steps = [f"Head {cardinal(best['bearing'])} for {dist_km} km toward {best['name']}"]
            eta_min = None
        
        # Store the user-specific route
        store_user_route(alert_id, user, best, steps, dist_km, eta_min)

    # Advice + response
    advice = "Move to higher ground. Avoid flooded roads. Bring ID, meds, water."
    if lang == "ms":
        advice = "Naik ke kawasan tinggi. Elak jalan banjir. Bawa IC, ubat, air."

    sms_message = (f"[ResQnow] Flood warning. Nearest shelter: {best['name']} {dist_km}km. "
                   f"Route: {'; '.join(steps)}. Advice: {advice}")

    # Send alert via SNS with support for Email subject.
    sms_error = None
    try:
        # Create a subject line for Email subscribers
        email_subject = "ResQnow Flood Alert Warning"
        
        if SNS_TOPIC_ARN and SNS_TOPIC_ARN.startswith('arn:aws:sns:'):
            # Publish to the topic. The 'Subject' parameter will be used by Email subscribers.
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=sms_message,
                Subject=email_subject
            )
        elif phone:
            # Fallback: direct publish to a phone number if no topic is configured.
            sns_client.publish(PhoneNumber=phone, Message=sms_message)
        else:
            sms_error = "No SNS topic or phone number configured for alerts."
    except Exception as e:
        sms_error = str(e)

    resp = {
        "affected": user_inside,
        "shelter": {"id": best["id"], "name": best["name"], "lat": best["lat"], "lng": best["lng"]},
        "distance_km": dist_km,
        "eta_min": eta_min,
        "steps": steps,
        "advice": advice,
        "sms": sms_message,
        "sms_error": sms_error
    }
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(resp)
    }

def store_alert(alert):
    """Stores the validated alert in the Alerts table."""
    table = dynamodb.Table(ALERTS_TBL)
    
    item = {
        'alert_id': alert['alert_id'],
        'timestamp': alert['timestamp'],
        'status': alert.get('status', 'CONFIRMED'),
        'event_type': alert['event_type'],
        'description': alert['description'],
        'location': alert['location'],
        'priority': alert.get('priority', 'medium'),
        'population_estimate': alert.get('population_estimate', 0)
    }
    
    table.put_item(Item=item)

def calculate_routes(alert):
    """
    Calculates evacuation routes.
    In MOCK_MODE, returns predefined data for demo stability.
    Otherwise, would call Google Maps/AWS Location Service here.
    """
    if MOCK_MODE:
        print("Operating in MOCK_MODE for reliable demo.")
        # Pre-calculated route from Jalan Ampang to KL Sports Complex
        return [
            {
                'route_id': str(uuid.uuid4()),
                'alert_id': alert['alert_id'],
                'shelter_id': 'center-1', # KL Sports Complex
                'shelter_name': 'KL Sports Complex',
                'dest_lat': 3.1486,
                'dest_lng': 101.7081,
                'distance_km': 4.5,
                'eta_min': 12,
                'steps': ['Head northeast on Jalan Ampang for 2.1 km', 'Turn right onto Jalan Tun Razak for 1.2 km'],
                'status': 'CALCULATED',
                'calculated_at': datetime.utcnow().isoformat() + 'Z',
                'user_specific': False
            }
        ]
    else:
        # TODO: Integrate with Google Maps API or AWS Location Service
        # This would be your real implementation
        return []

def store_routes(alert_id, routes):
    """Stores the calculated routes in the EvacuationRoutes table."""
    table = dynamodb.Table(ROUTES_TABLE)
    
    for route in routes:
        table.put_item(Item=route)