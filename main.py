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

REPLIES_LIMIT = 20


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


#
#   Collect the replies to a tweet
#
def get_replies(profile,tweet_id,api):

    # Get all the replies to a profile

    data = tw.Cursor(api.search_tweets, q='to:{}'.format(profile),since_id=tweet_id, tweet_mode='extended').items(REPLIES_LIMIT)
    replies = []

    # For each reply to a profile let's check if is a reply to the tweet with tweet_id
    # If yes let's add it to the replies list

    for reply in data:
        if not hasattr(reply, 'in_reply_to_status_id_str'):
            continue
        if reply.in_reply_to_status_id == tweet_id:
            replies.append(reply)
 
    return replies


if __name__ == "__main__":

    load_dotenv()

    api = connection_to_api()

    # Loading of profiles screen names and the keywords
    
    profiles, keywords = load_filters()

    print("  Tweets collector script  ")

    print("[*] profiles loaded: ", len(profiles))
    print("[*] keywords loaded: ", len(keywords))

    # Quey building: (keyword1 OR keyword2 OR ...)

    keywords_query = "("

    for keyword,i in zip(keywords, range(len(keywords))):
        keywords_query += keyword
        if(i<len(keywords)-1):
            keywords_query += " OR "

    keywords_query += ")"

    # Set the fromDate as yesterday

    fromDate = datetime.date.today() - datetime.timedelta(1)

    print("[*] from date: ", fromDate.strftime("%Y%m%d%H%M"))

    # Create and open the output files: one for the tweets collected and another for the replies

    file_tweets = open("output/tweets.json", "w")
    file_replies = open("output/replies.json", "w")


    for profile in profiles:
        print("[*] collecting tweets of: ", profile)

        # Query building: (keyword1 OR keyword2 OR ...) from profile_name

        query = keywords_query + " from: " + profile

        # Get all the tweets that contains one or more keywords from each profile in filters/profiles.txt

        for tweet in tw.Cursor(api.search_full_archive,label=os.getenv("DEV_ENVIRONMENT") ,fromDate= fromDate.strftime("%Y%m%d%H%M"), query= query).items():

            print("[*] loading replies for tweet_id: ", tweet.id, "in ", profile)

            # For each tweet let's get all the replies associated

            replies = get_replies(profile,tweet.id,api)

            print("> replies colected: ", len(replies))

            # Print all the replies in output/replies.json

            for reply in replies:
                file_replies.write(json.dumps(reply._json, indent=4))
                file_replies.write("\n\n\n")

            # Add the tweet in output/tweets.json

            file_tweets.write(json.dumps(tweet._json, indent=4))
            file_tweets.write("\n\n\n")