import tweepy
import boto3
import json
import requests
import time

# --- Twitter API ---
bearer_token = "AAAAAAAAAAAAAAAAAAAAAIv94AEAAAAA8JJcRJswFhZ8qHNmKicRfr9CziE%3DIYs2GsdChH5SicGfhpCtC5Uy84BBALhGxRXNe0fCDsmEfji1Zv"
client = tweepy.Client(bearer_token=bearer_token)

# --- AWS Kinesis Client ---
kinesis = boto3.client(
    "kinesis",
    region_name="ap-southeast-1" #SG server
)
stream_name = "DisasterDataStream"

# --- OpenWeatherMap API ---
OWM_API_KEY = "a00d543e9ee9d53004281d0b7b55aa10"
OWM_URL = "https://api.openweathermap.org/data/2.5/weather"

# --- Keywords ---
disaster = "(flood OR banjir OR landslide OR earthquake OR haze OR fire OR forestfire)"
cities = "(Kuala Lumpur OR KL OR Penang OR Johor Bahru OR Kota Kinabalu OR Kuching OR Ipoh OR Alor Setar OR Kota Bharu OR Kuala Terengganu OR Kuantan OR Melaka OR Kangar)"
states = "(Selangor OR Penang OR Johor OR Kedah OR Kelantan OR Malacca OR Pahang OR Sabah OR Sarawak OR Terengganu OR Labuan OR Putrajaya)"

max_results = 10  # per request
seen_tweet_ids = set()

# --- OpenWeatherMap cross-check ---
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

# --- Send to Kinesis ---
def send_to_kinesis(tweet, location, weather_desc):
    if tweet.id in seen_tweet_ids:
        return
    payload = {
        "id": tweet.id,
        "text": tweet.text,
        "location": location,
        "weather_desc": weather_desc
    }
    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(payload).encode("utf-8"),
        PartitionKey=str(tweet.id)
    )
    seen_tweet_ids.add(tweet.id)
    print(f"✅ Sent to Kinesis (Weather OK: {weather_desc}): {tweet.text}")

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
    query_cities = f"{disaster} {cities} -is:retweet lang:en"
    city_tweets = fetch_tweets(query_cities)

    if city_tweets:
        print(f"Found {len(city_tweets)} city-level tweets")
        for tweet in city_tweets:
            # Try to cross-check with a few key cities
            for loc in ["Kuala Lumpur", "Penang", "Johor Bahru", "Kota Kinabalu", "Kuching"]:
                valid, weather_desc = check_weather(loc)
                if valid:
                    send_to_kinesis(tweet, loc, weather_desc)
                    break  # only need one valid location
    else:
        print("⚠️ No city-level tweets found, trying state-level fallback")
        query_states = f"{disaster} {states} -is:retweet lang:en"
        state_tweets = fetch_tweets(query_states)
        if state_tweets:
            print(f"Found {len(state_tweets)} state-level tweets")
            for tweet in state_tweets:
                for loc in ["Selangor", "Sabah", "Sarawak", "Johor", "Kelantan"]:
                    valid, weather_desc = check_weather(loc)
                    if valid:
                        send_to_kinesis(tweet, loc, weather_desc)
                        break
        else:
            print("❌ No relevant tweets found at both city and state levels")

if __name__ == "__main__":
    main()
