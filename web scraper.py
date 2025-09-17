import tweepy

bearer_token = "AAAAAAAAAAAAAAAAAAAAAIv94AEAAAAA5ShdVc2N1KMkVDhlXuH1fMJNz84%3DqChG0VVJNjG7PybSUNX44mrZgjbi9TdwnW0ffvJ3jGMyhq2RCf"
client = tweepy.Client(bearer_token=bearer_token)

# Disaster keywords
disaster = "(flood OR banjir OR landslide OR earthquake OR haze OR fire OR forestfire)"

# Major cities in Malaysia
cities = """(
Kuala Lumpur OR KL OR Shah Alam OR Klang OR Subang Jaya OR Petaling Jaya OR Kajang OR Seremban OR Putrajaya OR
George Town OR Penang OR Butterworth OR Bukit Mertajam OR Seberang Perai OR
Johor Bahru OR Iskandar Puteri OR Batu Pahat OR Kluang OR Muar OR Segamat OR
Kota Kinabalu OR Sandakan OR Tawau OR Lahad Datu OR Kudat OR
Kuching OR Miri OR Sibu OR Bintulu OR
Ipoh OR Taiping OR Teluk Intan OR
Alor Setar OR Sungai Petani OR Kulim OR
Kota Bharu OR Tanah Merah OR
Kuala Terengganu OR Kemaman OR Dungun OR
Kuantan OR Bentong OR
Melaka City OR Ayer Keroh OR Jasin OR
Kangar OR Labuan
)"""

# State-level keywords as fallback
states = """(
Malaysia OR Selangor OR Penang OR Perlis OR Johor OR Kedah OR Kelantan OR Malacca OR
Negeri Sembilan OR Pahang OR Sabah OR Sarawak OR Terengganu OR Labuan OR Putrajaya
)"""

# First: try city-level query
query_cities = disaster + " " + cities + " -is:retweet lang:en"
tweets = client.search_recent_tweets(query=query_cities, max_results=10)

if tweets.data:
    print("✅ Found tweets at CITY level:\n")
    for tweet in tweets.data:
        print(tweet.text, "\n")
else:
    print("⚠️ No city-level results, trying STATE level...\n")
    # Fallback: try state-level query
    query_states = disaster + " " + states + " -is:retweet lang:en"
    tweets = client.search_recent_tweets(query=query_states, max_results=10)
    
    if tweets.data:
        print("✅ Found tweets at STATE level:\n")
        for tweet in tweets.data:
            print(tweet.text, "\n")
    else:
        print("❌ No relevant tweets found at both city and state levels.")
