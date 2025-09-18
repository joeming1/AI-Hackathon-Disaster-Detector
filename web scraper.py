import tweepy
import boto3
import json

# --- X (Twitter) API ---
bearer_token = "AAAAAAAAAAAAAAAAAAAAAIv94AEAAAAA8JJcRJswFhZ8qHNmKicRfr9CziE%3DIYs2GsdChH5SicGfhpCtC5Uy84BBALhGxRXNe0fCDsmEfji1Zv"

client = tweepy.Client(bearer_token=bearer_token)

# --- AWS Kinesis Client ---
kinesis = boto3.client(
    "kinesis",
    region_name="ap-southeast-5"  # Malaysia region
)

stream_name = "DisasterTweetsStream"

# Disaster keywords
disaster = "(flood OR banjir OR landslide OR earthquake OR haze OR fire OR forestfire)"

cities = """(Kuala Lumpur)"""

query_cities = disaster + " " + cities + " -is:retweet lang:en"

# Fetch tweets
tweets = client.search_recent_tweets(query=query_cities, max_results=10)

if tweets.data:
    for tweet in tweets.data:
        print("Sending tweet:", tweet.text)

        # Prepare tweet payload
        payload = {
            "id": tweet.id,
            "text": tweet.text,
        }

        # Send to AWS Kinesis
        kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(payload).encode("utf-8"),
            PartitionKey=str(tweet.id)  # partition by tweet ID
        )

    print("✅ Successfully pushed tweets to Kinesis!")

else:
    print("⚠️ No tweets found for cities.")
