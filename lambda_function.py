# lambda_function.py
# Env vars expected:
#   SHELTERS_TBL=Shelters
#   ALERTS_TBL=Alerts
#   RESIDENTS_TBL=Resident_info
#   MAX_STEPS=5
#   GMP_API_KEY=<Google Maps Directions API key>   # optional; bearing fallback if absent
#   SNS_REGION=us-east-1                           # for SMS
#   SNS_TOPIC_ARN=arn:aws:sns:us-east-1:...:ResQnowDemo   # optional

import os, re, json, math, urllib.request, urllib.parse, boto3

dynamodb = boto3.resource("dynamodb")

SHELTERS_TBL   = os.getenv("SHELTERS_TBL", "Shelters")
ALERTS_TBL     = os.getenv("ALERTS_TBL", "Alerts")
RESIDENTS_TBL  = os.getenv("RESIDENTS_TBL", "Resident_info")
MAX_STEPS      = int(os.getenv("MAX_STEPS", "5"))
GMP_API_KEY    = os.getenv("GMP_API_KEY")  # if not set → we use bearing-only fallback

SNS_REGION     = os.getenv("SNS_REGION", "us-east-1")
SNS_TOPIC_ARN  = os.getenv("SNS_TOPIC_ARN")  # optional
sns            = boto3.client("sns", region_name=SNS_REGION)

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
    # ring: [[lng,lat], ...]  (closed)
    x, y = pt["lng"], pt["lat"]
    inside = False
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        if ((y1 > y) != (y2 > y)) and (x < (x2 - x1) * (y - y1) / (y2 - y1 + 1e-12) + x1):
            inside = not inside
    return inside

def parse_poly(poly_str):
    # Stored as a JSON string in DynamoDB
    gj = json.loads(poly_str)
    return gj["coordinates"][0]  # exterior ring (list of [lng,lat])

def bearing_deg(a, b):
    lat1 = math.radians(a["lat"]); lat2 = math.radians(b["lat"])
    dlon = math.radians(b["lng"] - a["lng"])
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1)*math.sin(lat2) - math.sin(lat1)*math.cos(lat2)*math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0

def cardinal(deg):
    dirs = ["N","NE","E","SE","S","SW","W","NW","N"]
    return dirs[int((deg + 22.5)//45)]

def strip_html(s):  # Google returns html_instructions
    return re.sub("<[^>]+>", "", s or "")

# ---------- Google Directions (optional) ----------
def gmaps_steps(user, dest, lang="en", max_steps=5):
    if not GMP_API_KEY:
        return None  # no key configured → skip
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

# ---------- main handler ----------
def lambda_handler(event, context):
    # Parse body (supports both string and dict)
    body_raw = event.get("body")
    if isinstance(body_raw, str):
        try:
            body = json.loads(body_raw or "{}")
        except Exception:
            return {"statusCode": 400, "body": json.dumps({"error": "BAD_JSON"})}
    else:
        body = body_raw or {}

    # Accept either:
    #  A) { "user": { "lat": ..., "lng": ... }, "alert_id": "...", ... }
    #  B) { "phone": "+60123...", "alert_id": "...", ... }  -> we look up location in RESIDENTS_TBL
    alert_id = body.get("alert_id")
    lang     = body.get("lang", "en")
    phone    = body.get("phone")
    require_inside = bool(body.get("require_inside", False))  # optional guard

    if not alert_id:
        return {"statusCode": 400, "body": json.dumps({"error": "MISSING_FIELDS", "hint": "alert_id"})}

    if "user" in body and body["user"] is not None:
        user = {"lat": float(body["user"]["lat"]), "lng": float(body["user"]["lng"])}
    elif phone:
        # lookup user location from DynamoDB
        residents_tbl = dynamodb.Table(RESIDENTS_TBL)
        r = residents_tbl.get_item(Key={"phone": phone}).get("Item")
        if not r or "lat" not in r or "lng" not in r:
            return {"statusCode": 404, "body": json.dumps({"error":"USER_NOT_FOUND_OR_NO_LOCATION"})}
        user = {"lat": float(r["lat"]), "lng": float(r["lng"])}
        # if client didn't send lang, try table
        lang = body.get("lang", r.get("lang", "en"))
    else:
        return {"statusCode": 400, "body": json.dumps({"error": "MISSING_FIELDS", "hint": "user or phone"})}

    alerts_tbl   = dynamodb.Table(ALERTS_TBL)
    shelters_tbl = dynamodb.Table(SHELTERS_TBL)

    # 1) Load active alert + polygon
    alert = alerts_tbl.get_item(Key={"alert_id": alert_id}).get("Item")
    if not alert or alert.get("status") != "active":
        return {"statusCode": 404, "body": json.dumps({"error": "ALERT_NOT_FOUND"})}

    ring = parse_poly(alert["polygon"])

    # Optional: only proceed if the user is inside the polygon
    if require_inside and not point_in_polygon(user, ring):
        return {"statusCode": 200, "body": json.dumps({"affected": False, "reason": "USER_OUTSIDE_POLYGON"})}

    # 2) Load shelters and pick nearest outside the polygon
    items = []
    scan = shelters_tbl.scan(Limit=500)
    items.extend(scan.get("Items", []))
    while "LastEvaluatedKey" in scan:
        scan = shelters_tbl.scan(Limit=500, ExclusiveStartKey=scan["LastEvaluatedKey"])
        items.extend(scan.get("Items", []))

    candidates = []
    for it in items:
        s = {
            "id":   it["shelter_id"],
            "name": it["name"],
            "lat":  float(it["lat"]),
            "lng":  float(it["lng"]),
        }
        if not point_in_polygon({"lat": s["lat"], "lng": s["lng"]}, ring):
            s["crow_km"] = haversine_km(user, s)
            s["bearing"] = bearing_deg(user, s)
            candidates.append(s)

    if not candidates:
        return {"statusCode": 409, "body": json.dumps({"error": "NO_SAFE_SHELTERS"})}

    candidates.sort(key=lambda x: x["crow_km"])
    best = candidates[0]

    # 3) Try Google Directions → else compass/bearing fallback
    steps = dist_km = eta_min = None
    try:
        result = gmaps_steps(user, best, lang=lang, max_steps=MAX_STEPS)
        if result:
            steps, dist_km, eta_min = result
    except Exception:
        steps = None  # fall through to bearing

    if not steps:
        dist_km = round(best["crow_km"], 1)
        steps   = [f"Head {cardinal(best['bearing'])} for {dist_km} km toward {best['name']}"]
        eta_min = None

    # 4) Advice + response
    advice = "Move to higher ground. Avoid flooded roads. Bring ID, meds, water."
    if lang == "ms":
        advice = "Naik ke kawasan tinggi. Elak jalan banjir. Bawa IC, ubat, air."

    sms = (f"[ResQnow] Flood warning. Nearest shelter: {best['name']} {dist_km}km. "
           f"Route: {'; '.join(steps)}. Advice: {advice}")

    # 5) Optional SMS auto-send
    #    - If a phone was provided (or looked-up), send direct SMS
    #    - Additionally, if SNS_TOPIC_ARN is set, also publish to that topic
    try:
        if phone:
            sns.publish(PhoneNumber=phone, Message=sms)
        if SNS_TOPIC_ARN:
            sns.publish(TopicArn=SNS_TOPIC_ARN, Message=sms)
    except Exception as e:
        # don't fail the API if SMS fails; surface in response
        sms_error = str(e)
    else:
        sms_error = None

    resp = {
        "affected": point_in_polygon(user, ring),
        "shelter": {"id": best["id"], "name": best["name"], "lat": best["lat"], "lng": best["lng"]},
        "distance_km": dist_km,
        "eta_min": eta_min,
        "steps": steps,
        "advice": advice,
        "sms": sms,
        "sms_error": sms_error
    }
    return {
        "statusCode": 200,
        "headers": {"Content-Type":"application/json","Access-Control-Allow-Origin":"*"},
        "body": json.dumps(resp)
    }

