import tweepy
import boto3
import json
import requests
import re

# --- Twitter API ---
bearer_token = "AAAAAAAAAAAAAAAAAAAAAIv94AEAAAAA8JJcRJswFhZ8qHNmKicRfr9CziE%3DIYs2GsdChH5SicGfhpCtC5Uy84BBALhGxRXNe0fCDsmEfji1Zv"
client = tweepy.Client(bearer_token=bearer_token)

# --- AWS Kinesis Client ---
kinesis = boto3.client("kinesis", region_name="ap-southeast-1")
stream_name = "DisasterDataStream"

# --- OpenWeatherMap API ---
OWM_API_KEY = "a00d543e9ee9d53004281d0b7b55aa10"
OWM_URL = "https://api.openweathermap.org/data/2.5/weather"

# --- Keywords ---
disaster_keywords = {
    "flood": ["flood", "banjir"],
    "landslide": ["landslide"],
    "fire": ["fire", "forestfire"],
    "haze": ["haze"],
    "earthquake": ["earthquake"]
}

cities = "(Kuala Lumpur OR KL OR Penang OR Johor Bahru OR Kota Kinabalu OR Kuching OR Ipoh OR Alor Setar OR Kota Bharu OR Kuala Terengganu OR Kuantan OR Melaka OR Kangar)"
states = "(Selangor OR Penang OR Johor OR Kedah OR Kelantan OR Malacca OR Pahang OR Sabah OR Sarawak OR Terengganu OR Labuan OR Putrajaya)"

max_results = 10
seen_tweet_ids = set()
header_sent = False  # track if header already sent

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

# --- Detect disaster type ---
def detect_disaster(text):
    text_lower = text.lower()
    for disaster, keywords in disaster_keywords.items():
        if any(word in text_lower for word in keywords):
            return disaster
    return "unknown"

# --- Send to Kinesis (as CSV row) ---
def send_to_kinesis(tweet, location, weather_desc):
    global header_sent
    if tweet.id in seen_tweet_ids:
        return

    disaster_type = detect_disaster(tweet.text)
    clean_text = re.sub(r"[\n\r,]", " ", tweet.text)

    # Add header only once
    if not header_sent:
        header = "id,text,location,disaster_type,weather_desc\n"
        kinesis.put_record(
            StreamName=stream_name,
            Data=header.encode("utf-8"),
            PartitionKey="header"
        )
        header_sent = True

    # Format row
    payload = f"{tweet.id},{clean_text},{location},{disaster_type},{weather_desc}\n"
    kinesis.put_record(
        StreamName=stream_name,
        Data=payload.encode("utf-8"),
        PartitionKey=str(tweet.id)
    )

    seen_tweet_ids.add(tweet.id)
    print(f"✅ Sent: {tweet.id} ({disaster_type}, {weather_desc})")

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
    query_cities = f"({' OR '.join(sum(disaster_keywords.values(), []))}) {cities} -is:retweet lang:en"
    city_tweets = fetch_tweets(query_cities)

    if city_tweets:
        for tweet in city_tweets:
            for loc in ["Kuala Lumpur", "Penang", "Johor Bahru", "Kota Kinabalu", "Kuching",
                        "Ipoh", "Alor Setar", "Kota Bharu", "Kuala Terengganu", "Kuantan",
                        "Melaka", "Kangar"]:
                valid, weather_desc = check_weather(loc)
                if valid:
                    send_to_kinesis(tweet, loc, weather_desc)
                    break
    else:
        query_states = f"({' OR '.join(sum(disaster_keywords.values(), []))}) {states} -is:retweet lang:en"
        state_tweets = fetch_tweets(query_states)
        if state_tweets:
            for tweet in state_tweets:
                for loc in ["Selangor", "Sabah", "Sarawak", "Johor", "Kelantan",
                            "Kedah", "Malacca", "Pahang", "Terengganu",
                            "Labuan", "Putrajaya"]:
                    valid, weather_desc = check_weather(loc)
                    if valid:
                        send_to_kinesis(tweet, loc, weather_desc)
                        break

# --- Lambda handler ---
def lambda_handler(event, context):
    main()
    return {"statusCode": 200, "body": json.dumps("Run complete")}