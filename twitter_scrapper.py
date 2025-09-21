import tweepy
import requests
import re
import json
import boto3
import io
import csv
from datetime import datetime

# --- Hard-coded configuration ---
bearer_token = "AAAAAAAAAAAAAAAAAAAAAP4m4QEAAAAAlUh56Sk3VzQ8u%2Fg7EEHDPUradio%3DoTMkjAucjrBSlGimBmKNTcrklGQCiPTmidnPeZv9BBcwcxEG2i"
kinesis_stream = "DisasterDataStream"
s3_bucket = "twitterdata4hackathon"
owm_api_key = "a00d543e9ee9d53004281d0b7b55aa10"
seen_file_key = "seen_tweet_ids.json"

# Always produce a per-run CSV in S3 with header
WRITE_BATCH_CSV_TO_S3 = True

# --- AWS Clients ---
kinesis = boto3.client("kinesis", region_name="ap-southeast-1")
s3 = boto3.client("s3")

# --- Twitter Client ---
client = tweepy.Client(bearer_token=bearer_token)

# --- Disaster keywords / states (unchanged) ---
# --- Disaster keywords / states (unchanged) ---
disaster_keywords = {
    "flood": ["flood", "banjir"],
    "landslide": ["landslide"],
    "storm": ["storm"],
    "haze": ["haze"],
    "earthquake": ["earthquake"]
}

states = ["Kuala Lumpur", "Johor Bahru", "Kota Bharu", "Kuantan", "George Town", "Alor Setar", "Kuching", "Kota Kinabalu"]
# <=100 labels enforcement
ALLOWED_LABELS = {f"{d}_{s}" for d in list(disaster_keywords.keys()) + ["unknown"] for s in states}
ALLOWED_LABELS.add("other_other")

max_results = 10

# -------- helpers (same as before except where noted) --------
def load_seen_ids():
    try:
        obj = s3.get_object(Bucket=s3_bucket, Key=seen_file_key)
        return set(json.loads(obj["Body"].read()))
    except Exception:
        return set()

def save_seen_ids(seen_ids):
    try:
        s3.put_object(Bucket=s3_bucket, Key=seen_file_key, Body=json.dumps(list(seen_ids)).encode("utf-8"))
    except Exception as e:
        print(f"Error saving seen IDs: {e}")

def check_weather(location):
    try:
        params = {"q": location, "appid": owm_api_key, "units": "metric"}
        r = requests.get("https://api.openweathermap.org/data/2.5/weather", params=params, timeout=8)
        if r.status_code == 200:
            w = r.json()["weather"][0]["main"].lower()
            return any(x in w for x in ["rain", "storm", "haze", "cloud"])
        return False
    except Exception as e:
        print(f"Weather API error: {e}")
        return False

def detect_disaster(text):
    t = text.lower()
    for disaster, kws in disaster_keywords.items():
        if any(k in t for k in kws):
            return disaster
    return "Unknown"

def normalize_label(disaster, state):
    cand = f"{disaster}_{state}"
    return cand if cand in ALLOWED_LABELS else "Other_Other"

def send_to_kinesis_csv(label, text):
    try:
        buf = io.StringIO()
        csv.writer(buf).writerow([label, text])
        kinesis.put_record(
            StreamName=kinesis_stream,
            Data=buf.getvalue().encode("utf-8"),
            PartitionKey=label.split("_")[0]
        )
        # Print confirmation to terminal/log
        print(f"Sent to Kinesis → Label: {label} | Text: {text[:80]}...")
    except Exception as e:
        print(f"Error sending to Kinesis: {e}")


def send_kinesis_header_every_run():
    # REMOVE sending header into the stream
    # If you only want header in the S3 CSV, don't send it to Kinesis
    pass


def write_batch_csv_to_s3_with_header(rows):
    # Always write a NEW object with header per run (avoids mid-file header issues)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"tweets_batch_{ts}.csv"
    try:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["Label", "Text"])    # ALWAYS write header
        for r in rows:
            w.writerow(r)
        s3.put_object(Bucket=s3_bucket, Key=key, Body=buf.getvalue().encode("utf-8"))
        return key
    except Exception as e:
        print(f"Error writing CSV to S3: {e}")
        return None

def fetch_tweets(query):
    try:
        tweets = client.search_recent_tweets(query=query, max_results=max_results)
        return tweets.data if tweets and tweets.data else []
    except Exception as e:
        print(f"Error fetching tweets: {e}")
        return []

# ---------------- Lambda handler ----------------
def lambda_handler(event, context):
    seen_tweet_ids = load_seen_ids()

    # Build query parts
    flat_keywords = sorted(set(sum(disaster_keywords.values(), [])))
    keyword_expr = " OR ".join(flat_keywords)
    lang_expr = "(lang:en OR lang:ms)"

    batch_rows = []
    for state in states:
        query = f"({keyword_expr}) \"{state}\" {lang_expr} -is:retweet"
        tweets = fetch_tweets(query)

        weather_ok = check_weather(state)

        for tw in tweets:
            if tw.id in seen_tweet_ids:
                continue
            if not weather_ok:
                continue

            disaster = detect_disaster(tw.text)
            clean_text = re.sub(r"[\n\r]+", " ", tw.text).strip()
            label = normalize_label(disaster, state)

            # Stream CSV row to Kinesis
            send_to_kinesis_csv(label, clean_text)

            # Accumulate for S3 CSV
            if WRITE_BATCH_CSV_TO_S3:
                batch_rows.append((label, clean_text))

            seen_tweet_ids.add(tw.id)

    save_seen_ids(seen_tweet_ids)

    s3_key = None
    if WRITE_BATCH_CSV_TO_S3 and batch_rows:
        s3_key = write_batch_csv_to_s3_with_header(batch_rows)

    return {
        "statusCode": 200,
        "body": f"Emitted rows to Kinesis; S3 CSV: {s3_key if s3_key else 'disabled or empty batch'}"
    }
