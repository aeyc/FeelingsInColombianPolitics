import tweepy as tw
import json
from dotenv import load_dotenv
import os
from datetime import timezone
import datetime
import time

#
#   Limit parameters for testing (if you are not using a premium account)
#

RESULTS_LIMIT = 10
RESULT_PER_PAGE = 11


#
#   Method for establish a connection whit Twitter API
#
def connection_to_api():
    auth = tw.OAuthHandler(os.getenv("API_KEY"), os.getenv("API_KEY_SECRET"))
    client = tw.Client(bearer_token=os.getenv("BEARER_TOKEN"))
    api = tw.API(auth, wait_on_rate_limit=True)
    return api,client


#
#   Load profiles screen name from profiles.txt and keywords from keywords.txt
#
def load_filters():
    profiles = []
    keywords = []
    file_profiles = open("filters/profiles.txt", "r")
    file_keywords = open("filters/keywords.txt", "r")
    for line in file_profiles:
        profiles.append(line.rstrip('\n'))
    for line in file_keywords:
        keywords.append(line.rstrip('\n'))
    return profiles, keywords


#
#   Collect the replies to a tweet
#
def get_replies(tweet_id):
    for reply in tw.Paginator(client.search_recent_tweets,query="conversation_id:{}".format(tweet_id),tweet_fields=["created_at"], max_results=10).flatten(limit=RESULT_PER_PAGE):
        parsed = {}
        parsed['id'] = reply.id
        parsed['created_at'] = str(reply.created_at)
        parsed['text'] = reply.text
        file_replies.write(json.dumps(parsed, indent=4))


if __name__ == "__main__":

    start_time = time.time()

    load_dotenv()

    api,client = connection_to_api()

    # Loading of profiles screen names and the keywords
    
    profiles, keywords = load_filters()

    print("\n\n  Tweets collector script  \n\n")

    print("[INFO] profiles loaded: ", len(profiles))
    print("[INFO] keywords loaded: ", len(keywords))


    fromDate = datetime.date.today() - datetime.timedelta(1)


    print("[INFO] start date: ", fromDate.strftime("%Y%m%d%H%M"))

    print("\n")

    # Create and open the output files: one for the tweets collected and another for the replies

    file_tweets = open("output/tweets.json", "w")
    file_replies = open("output/replies.json", "w")

    tweets_id = []

    # For each profile I get all the tweets from a certain date
    for profile in profiles:

        # Get the user_id of a screen name

        user_id = client.get_user(username=profile)[0].id

        print("Collecting replies of",profile, "...")

        for tweet in tw.Paginator(client.get_users_tweets,id = user_id, tweet_fields=["created_at"], exclude=["retweets", "replies"],start_time=fromDate.strftime("%Y-%m-%dT%H:%M:%SZ"),max_results=RESULTS_LIMIT).flatten(limit=RESULT_PER_PAGE):
            for keyword in keywords:
                if keyword in tweet.text:
                    parsed = {}
                    parsed['id'] = tweet.id
                    parsed['created_at'] = str(tweet.created_at)
                    parsed['text'] = tweet.text
                    file_tweets.write(json.dumps(parsed, indent=4))
                    tweets_id.append(tweet.id)
    
    print("[*] total tweets collected: ", len(tweets_id))
    print("\n")

    for tweet_id in tweets_id:
        print("Collecting replies for tweet_id: ",tweet_id, "...")
        
        rpl_count = 0

        for reply in tw.Paginator(client.search_recent_tweets,query="conversation_id:{}".format(tweet_id),tweet_fields=["created_at"], max_results=10).flatten(limit=RESULT_PER_PAGE):
            parsed = {}
            parsed['id'] = reply.id
            parsed['created_at'] = str(reply.created_at)
            parsed['text'] = reply.text
            file_replies.write(json.dumps(parsed, indent=4))
            rpl_count += 1

        print("[*] replies collected: ", rpl_count, "\n")

        end_time = time.time()

        print("---- Total execution time:",end_time-start_time," ----")
