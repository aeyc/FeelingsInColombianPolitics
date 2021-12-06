import tweepy as tw
import json
from dotenv import load_dotenv
import os
from datetime import datetime as dt,timezone
import datetime
import time
import pandas as pd

#
#   Limit parameters for testing (if you are not using a premium account)
#

RESULTS_LIMIT = 10


#
#   Method for establish a connection whit Twitter API
#
def connection_to_api():
    auth = tw.OAuthHandler(os.getenv("API_KEY"), os.getenv("API_KEY_SECRET"))
    api = tw.API(auth, wait_on_rate_limit=True)
    client = tw.Client(bearer_token=os.getenv("BEARER_TOKEN"))
    return client, api


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

def log_print(msg):
    parsed = "[" + str(dt.now().strftime("%Y-%m-%d %H:%M:%S")) + "] " + msg.translate({ord('\n'): None}) + "\n"
    file_logs.write(parsed)
    print(msg)


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
    file_retweets = open("output/retweets.json", "w")

    tweets_id = []

    client,api= connection_to_api() # API authentication
    
    profiles, keywords = load_filters() # Loading of profiles screen names and the keywords

    fromDate = datetime.datetime.now(timezone.utc).replace(microsecond=0) - datetime.timedelta(1)

    log_print("\n\nTweets collector script\n\n")
    log_print("[INFO] profiles loaded: " + str(len(profiles)))
    log_print("[INFO] keywords loaded: " + str(len(keywords)))
    log_print("[INFO] start date: " + fromDate.strftime("%Y%m%d%H%M"))
    print("\n")

    #
    #   Algorithm
    #

    # Tweets collection (APIv2)

    for profile in profiles:

        # Get the user_id of a screen name

        user_id = client.get_user(username=profile)[0].id

        log_print("Collecting tweets of " + profile + "...")

        for tweet in tw.Paginator(client.get_users_tweets,id = user_id, tweet_fields=["author_id","created_at","public_metrics","entities","geo","lang"], exclude=["retweets", "replies"],start_time=fromDate.isoformat()).flatten(limit=RESULTS_LIMIT):
            for keyword in keywords:
                if keyword.casefold() in tweet.text.casefold():
                    parsed = {}
                    parsed['id'] = tweet.id
                    parsed['author_id'] = tweet.author_id
                    parsed['created_at'] = str(tweet.created_at)
                    parsed['text'] = tweet.text
                    parsed['public_metrics'] = tweet.public_metrics
                    parsed['entities'] = tweet.entities
                    parsed['geo'] = tweet.geo
                    parsed['lang'] = tweet.lang
                    file_tweets.write(json.dumps(parsed, indent=4))
                    tweets_id.append(tweet.id)

                    end_time_retweet = (datetime.datetime.fromisoformat(tweet.created_at.isoformat()) + datetime.timedelta(days = 7, minutes = 1)).isoformat()

                    now_time = (datetime.datetime.now(timezone.utc).replace(microsecond=0) - datetime.timedelta(minutes = 1)).isoformat()

                    if(end_time_retweet >= now_time):
                        end_time_retweet = now_time


                    log_print("Collecting retweets of " + str(tweet.id) + "...")

                    rtw_count = 0
                    
                    for retweet in tw.Paginator(client.search_recent_tweets,query="url:" + profile + " is:quote", start_time= tweet.created_at.isoformat(),end_time = end_time_retweet,tweet_fields=["author_id","created_at","public_metrics","entities","geo","lang","referenced_tweets"]).flatten(limit=RESULTS_LIMIT):
                        belongs_to_tweet = False
                        refs = []
                        for el in retweet.referenced_tweets:
                            if(el.id == tweet.id):
                                belongs_to_tweet = True
                            rt = {}
                            rt['type'] = el.type
                            rt['id'] = el.id
                            refs.append(rt)

                        if(belongs_to_tweet):
                            rtw_count += 1
                            parsed['id'] = retweet.id
                            parsed['author_id'] = retweet.author_id
                            parsed['tweet_id'] = tweet.id
                            parsed['created_at'] = str(retweet.created_at)
                            parsed['text'] = retweet.text
                            parsed['public_metrics'] = retweet.public_metrics
                            parsed['entities'] = retweet.entities
                            parsed['geo'] = retweet.geo
                            parsed['lang'] = retweet.lang
                            parsed['referenced_tweets'] = refs                        
                            file_retweets.write(json.dumps(parsed, indent=4))
                        
                    log_print("[*] retweets with quotes collected: " + str(rtw_count) + "\n")

    log_print("[*] total tweets collected: " + str(len(tweets_id)))
    print("\n")

    total_rpl_count = 0

    for tweet_id in tweets_id:

        # Replies collection (APIv2)
        
        log_print("Collecting replies for tweet_id: " + str(tweet_id) + "...")
        
        rpl_count = 0

        for reply in tw.Paginator(client.search_recent_tweets,query="conversation_id:{}".format(tweet_id),tweet_fields=["author_id","created_at","public_metrics","entities","geo","lang"]).flatten(limit=RESULTS_LIMIT):
            parsed = {}
            parsed['id'] = reply.id
            parsed['author_id'] = reply.author_id
            parsed['tweet_id'] = tweet_id
            parsed['created_at'] = str(reply.created_at)
            parsed['text'] = reply.text
            parsed['public_metrics'] = reply.public_metrics
            parsed['entities'] = reply.entities
            parsed['geo'] = reply.geo
            parsed['lang'] = reply.lang
            file_replies.write(json.dumps(parsed, indent=4))
            rpl_count += 1

        total_rpl_count += rpl_count

        log_print("[*] replies collected: " + str(rpl_count) + "\n")

    end_time = time.time()

    log_print("---- Total execution time:" + str(end_time-start_time) + " ----")

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
    file_retweets.close()