import tweepy
import boto3
import json
import requests
import csv
import io

# --- Twitter API ---
bearer_token = "YOUR_TWITTER_BEARE"
client = tweepy.Client(bearer_token=bearer_token)

# --- AWS Kinesis Firehose Client ---
firehose = boto3.client("firehose", region_name="ap-southeast-1")
firehose_name = "DisasterDataFirehose"  # Firehose must be configured to deliver to S3

# --- OpenWeatherMap API ---
OWM_API_KEY = "YOUR_OWM_KEY"
OWM_URL = "https://api.openweathermap.org/data/2.5/weather"

# --- Keywords ---
disaster_keywords = {
    "flood": ["flood", "banjir"],
    "landslide": ["landslide"],
    "earthquake": ["earthquake"],
    "haze": ["haze"],
    "fire": ["fire", "forestfire"]
}

cities = ["Kuala Lumpur", "Penang", "Johor Bahru", "Kota Kinabalu", "Kuching",
          "Ipoh", "Alor Setar", "Kota Bharu", "Kuala Terengganu", "Kuantan",
          "Melaka", "Kangar"]
states = ["Selangor", "Penang", "Johor", "Kedah", "Kelantan",
          "Malacca", "Pahang", "Sabah", "Sarawak", "Terengganu", "Labuan", "Putrajaya"]

max_results = 10
seen_tweet_ids = set()

# --- Detect disaster type ---
def detect_disaster(text):
    text_lower = text.lower()
    for dtype, keywords in disaster_keywords.items():
        if any(word in text_lower for word in keywords):
            return dtype
    return "unknown"

# --- Weather cross-check ---
def check_weather(location):
    try:
        params = {"q": location, "appid": OWM_API_KEY, "units": "metric"}
        r = requests.get(OWM_URL, params=params)
        if r.status_code == 200:
            data = r.json()
            weather = data["weather"][0]["main"].lower()
            description = data["weather"][0]["description"]
            if any(w in weather for w in ["rain", "storm", "haze", "cloud"]):
                return True, description
            else:
                return False, description
        else:
            return False, "API error"
    except Exception as e:
        return False, str(e)

# --- Send to Firehose in CSV format ---
def send_to_firehose(tweet, location, weather_desc):
    if tweet.id in seen_tweet_ids:
        return

    disaster_type = detect_disaster(tweet.text)

    # Build CSV row
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([tweet.id, tweet.text.replace("\n", " "), location, weather_desc, disaster_type])
    csv_data = output.getvalue()

    firehose.put_record(
        DeliveryStreamName=firehose_name,
        Record={"Data": csv_data.encode("utf-8")}
    )

    seen_tweet_ids.add(tweet.id)
    print(f"✅ Sent to Firehose CSV: {tweet.text[:50]}... ({disaster_type})")

# --- Fetch Tweets ---
def fetch_tweets(query):
    try:
        tweets = client.search_recent_tweets(query=query, max_results=max_results)
        return tweets.data if tweets.data else []
    except Exception as e:
        print("Error fetching tweets:", e)
        return []

# --- Main ---
def main():
    query_cities = f"({' OR '.join(disaster_keywords.keys())}) ({' OR '.join(cities)}) -is:retweet lang:en"
    city_tweets = fetch_tweets(query_cities)

    if city_tweets:
        print(f"Found {len(city_tweets)} city-level tweets")
        for tweet in city_tweets:
            for loc in cities:
                valid, weather_desc = check_weather(loc)
                if valid:
                    send_to_firehose(tweet, loc, weather_desc)
                    break
    else:
        print("⚠️ No city-level tweets found, trying state-level fallback")
        query_states = f"({' OR '.join(disaster_keywords.keys())}) ({' OR '.join(states)}) -is:retweet lang:en"
        state_tweets = fetch_tweets(query_states)
        if state_tweets:
            print(f"Found {len(state_tweets)} state-level tweets")
            for tweet in state_tweets:
                for loc in states:
                    valid, weather_desc = check_weather(loc)
                    if valid:
                        send_to_firehose(tweet, loc, weather_desc)
                        break
        else:
            print("❌ No relevant tweets found")

# --- Lambda entry point ---
def lambda_handler(event, context):
    main()
    return {"statusCode": 200, "body": json.dumps("Execution completed")}