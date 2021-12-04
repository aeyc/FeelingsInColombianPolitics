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


#
#   Method for establish a connection whit Twitter API
#
def connection_to_api():
    client = tw.Client(bearer_token=os.getenv("BEARER_TOKEN"))
    return client


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


if __name__ == "__main__":

    start_time = time.time()

    #
    #   Settings
    #

    load_dotenv()

    file_state = open(".appdata/state.json", "w")
    file_logs = open(".appdata/logs.txt", "w")
    file_tweets = open("output/tweets.json", "w")
    file_replies = open("output/replies.json", "w")

    tweets_id = []

    client = connection_to_api() # API authentication
    
    profiles, keywords = load_filters() # Loading of profiles screen names and the keywords

    fromDate = datetime.date.today() - datetime.timedelta(1)

    print("\n\n  Tweets collector script  \n\n")
    print("[INFO] profiles loaded: ", len(profiles))
    print("[INFO] keywords loaded: ", len(keywords))
    print("[INFO] start date: ", fromDate.strftime("%Y%m%d%H%M"))
    print("\n")

    #
    #   Algorithm
    #

    # For each profile I get all the tweets from a certain date
    for profile in profiles:

        # Get the user_id of a screen name

        user_id = client.get_user(username=profile)[0].id

        print("Collecting tweets of",profile, "...")

        for tweet in tw.Paginator(client.get_users_tweets,id = user_id, tweet_fields=["created_at","public_metrics","entities","geo"], exclude=["retweets", "replies"],start_time=fromDate.strftime("%Y-%m-%dT%H:%M:%SZ")).flatten(limit=RESULTS_LIMIT):
            for keyword in keywords:
                if keyword.casefold() in tweet.text.casefold():
                    parsed = {}
                    parsed['id'] = tweet.id
                    parsed['created_at'] = str(tweet.created_at)
                    parsed['text'] = tweet.text
                    parsed['public_metrics'] = tweet.public_metrics
                    parsed['entities'] = tweet.entities
                    parsed['geo'] = tweet.geo
                    file_tweets.write(json.dumps(parsed, indent=4))
                    tweets_id.append(tweet.id)
    
    print("[*] total tweets collected: ", len(tweets_id))
    print("\n")

    total_rpl_count = 0

    for tweet_id in tweets_id:
        print("Collecting replies for tweet_id: ",tweet_id, "...")
        
        rpl_count = 0

        for reply in tw.Paginator(client.search_recent_tweets,query="conversation_id:{}".format(tweet_id),tweet_fields=["created_at","public_metrics","entities","geo"], max_results=10).flatten(limit=RESULTS_LIMIT):
            parsed = {}
            parsed['id'] = reply.id
            parsed['in_reply_to_tweet_id'] = tweet_id
            parsed['created_at'] = str(reply.created_at)
            parsed['text'] = reply.text
            parsed['public_metrics'] = reply.public_metrics
            parsed['entities'] = reply.entities
            parsed['geo'] = reply.geo
            file_replies.write(json.dumps(parsed, indent=4))
            rpl_count += 1

        total_rpl_count += rpl_count

        print("[*] replies collected: ", rpl_count, "\n")


    end_time = time.time()

    print("---- Total execution time:",end_time-start_time," ----")

    state = {}
    state['last_run'] = datetime.date.today().strftime("%Y-%m-%dT%H:%M:%SZ")
    state['total_tweets_collected'] = len(tweets_id)
    state['total_replies_collected'] = total_rpl_count
    state['execution_time'] = end_time - start_time
    file_state.write(json.dumps(state, indent=4))

    file_state.close()
    file_logs.close()
    file_tweets.close()
    file_replies.close()