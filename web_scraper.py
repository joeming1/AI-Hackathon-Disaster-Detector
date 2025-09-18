import tweepy
import boto3
import json
import time

# --- Twitter API ---
bearer_token = "AAAAAAAAAAAAAAAAAAAAAIv94AEAAAAA8JJcRJswFhZ8qHNmKicRfr9CziE%3DIYs2GsdChH5SicGfhpCtC5Uy84BBALhGxRXNe0fCDsmEfji1Zv"
client = tweepy.Client(bearer_token=bearer_token)

# --- AWS Kinesis Client ---
kinesis = boto3.client(
    "kinesis",
    region_name="ap-southeast-1"
)

stream_name = "DisasterDataStream"

# --- Keywords ---
disaster = "(flood OR banjir OR landslide OR earthquake OR haze OR forestfire)"
cities = "(Kuala Lumpur OR KL OR Penang OR Johor Bahru OR Kota Kinabalu OR Kuching OR Ipoh OR Alor Setar OR Kota Bharu OR Kuala Terengganu OR Kuantan OR Melaka OR Kangar )"
states = "(Selangor OR Penang OR Johor OR Kedah OR Kelantan OR Malacca OR Pahang OR Sabah OR Sarawak OR Terengganu OR Labuan OR Putrajaya)"

max_results = 10  # per request

# --- Deduplication ---
seen_tweet_ids = set()

def send_to_kinesis(tweet):
    if tweet.id in seen_tweet_ids:
        return
    payload = {
        "id": tweet.id,
        "text": tweet.text,
    }
    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(payload).encode("utf-8"),
        PartitionKey=str(tweet.id)
    )
    seen_tweet_ids.add(tweet.id)
    print("✅ Sent to Kinesis:", tweet.text)

def fetch_tweets(query):
    try:
        tweets = client.search_recent_tweets(query=query, max_results=max_results)
        return tweets.data if tweets.data else []
    except Exception as e:
        print("Error fetching tweets:", e)
        return []

# --- Main ---
query_cities = f"{disaster} {cities} -is:retweet lang:en"
city_tweets = fetch_tweets(query_cities)

if city_tweets:
    print(f"Found {len(city_tweets)} city-level tweets")
    for tweet in city_tweets:
        send_to_kinesis(tweet)
else:
    print("⚠️ No city-level tweets found, trying state-level fallback")
    query_states = f"{disaster} {states} -is:retweet lang:en"
    state_tweets = fetch_tweets(query_states)
    if state_tweets:
        print(f"Found {len(state_tweets)} state-level tweets")
        for tweet in state_tweets:
            send_to_kinesis(tweet)
    else:
        print("❌ No relevant tweets found at both city and state levels")
