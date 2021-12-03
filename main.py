import tweepy as tw
import json
from dotenv import load_dotenv
import os
from datetime import timezone
import datetime
import time
from collections import namedtuple

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
def get_replies(conversation_id):
    for reply in tw.Paginator(client.search_recent_tweets, query="conversation_id:"+str(conversation_id), max_results=10).flatten(limit=RESULT_PER_PAGE):
        parsed = {}
        parsed['id'] = reply.id
        parsed['text'] = reply.text
        file_replies.write(json.dumps(parsed, indent=4))


if __name__ == "__main__":

    load_dotenv()

    api,client = connection_to_api()

    # Loading of profiles screen names and the keywords
    
    profiles, keywords = load_filters()

    print("  Tweets collector script  ")

    print("[*] profiles loaded: ", len(profiles))
    print("[*] keywords loaded: ", len(keywords))


    fromDate = datetime.date.today() - datetime.timedelta(1)

    print("[*] from date: ", fromDate.strftime("%Y%m%d%H%M"))

    # Create and open the output files: one for the tweets collected and another for the replies

    file_tweets = open("output/tweets.json", "w")
    file_replies = open("output/replies.json", "w")

    conversations = []

    # For each profile I get all the tweets from a certain date

    for profile in profiles:
        user_id = client.get_user(username=profile)[0].id
        for tweet in tw.Paginator(client.get_users_tweets,id = user_id, tweet_fields="conversation_id", max_results=RESULTS_LIMIT).flatten(limit=RESULT_PER_PAGE):
            for keyword in keywords:
                if keyword in tweet.text:
                    parsed = {}
                    parsed['id'] = tweet.id
                    parsed['conversation_id'] = tweet.conversation_id
                    parsed['text'] = tweet.text
                    file_tweets.write(json.dumps(parsed, indent=4))
                    conversations.append(tweet.conversation_id)

    for conversation_id in conversations:
        replies = get_replies(conversation_id)