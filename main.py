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

REPLIES_LIMIT = 5


#
#   Method for establish a connection whit Twitter API
#
def connection_to_api():
    auth = tw.OAuthHandler(os.getenv("API_KEY"), os.getenv("API_KEY_SECRET"))
    api = tw.API(auth, wait_on_rate_limit=True)
    return api


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

def get_replies(profile,tweet_id,api):
    data = tw.Cursor(api.search_tweets, q='to:{}'.format(profile),since_id=tweet_id, tweet_mode='extended').items(REPLIES_LIMIT)
    replies = []
    for reply in data:
        if not hasattr(reply, 'in_reply_to_status_id_str'):
            continue
        if reply.in_reply_to_status_id == tweet_id:
            #print("reply of tweet:{}".format(reply.full_text))
            replies.append(reply)
 
    return replies


if __name__ == "__main__":

    load_dotenv()

    api = connection_to_api()

    profiles, keywords = load_filters()

    print("  Tweets collector script  ")

    print("[*] profiles loaded: ", len(profiles))
    print("[*] keywords loaded: ", len(keywords))

    keywords_query = "("

    for keyword,i in zip(keywords, range(len(keywords))):
        keywords_query += keyword
        if(i<len(keywords)-1):
            keywords_query += " OR "

    keywords_query += ")"

    fromDate = datetime.date.today() - datetime.timedelta(1)

    print("[*] from date: ", fromDate.strftime("%Y%m%d%H%M"))

    file_tweets = open("output/tweets.json", "w")
    file_replies = open("output/replies.json", "w")


    for profile in profiles:
        print("[*] collecting tweets of: ", profile)
        query = keywords_query + " from: " + profile
        for tweet in tw.Cursor(api.search_full_archive,label=os.getenv("DEV_ENVIRONMENT") ,fromDate= fromDate.strftime("%Y%m%d%H%M"), query= query).items():

            print("[*] loading replies for tweet_id: ", tweet.id, "in ", profile)

            replies = get_replies(profile,tweet.id,api)

            print("> replies colected: ", len(replies))

            for reply in replies:
                file_replies.write(json.dumps(reply._json, indent=4))
                file_replies.write("\n\n\n")

            file_tweets.write(json.dumps(tweet._json, indent=4))
            file_tweets.write("\n\n\n")