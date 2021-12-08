import tweepy as tw
import json
from dotenv import load_dotenv
import os
from datetime import datetime as dt,timezone
import datetime
import time
import pandas as pd
import logging
from rich.console import Console
from rich.table import Column, Table

#
#   Limit parameters for testing (if you are not using an academic account)
#

RESULTS_LIMIT = 1

#
#   Method for establish a connection whit Twitter API
#
def connection_to_api():
    client = tw.Client(bearer_token=os.getenv("BEARER_TOKEN"), wait_on_rate_limit=True)
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


#
#   This method collects all the quotes retweets of a tweet
#   The output is saved in output/retweets.json
#
def get_retweets(profile, tweet_id):

    logging.info("Collecting retweets of " + str(tweet_id) + "...")

    rtw_count = 0

    url = "https://twitter.com/" + profile + "/status/" + str(tweet_id)

    for retweet in tw.Paginator(client.search_recent_tweets,query='url: "{}"'.format(url)  + " -is:reply -is:retweet is:quote", tweet_fields=["author_id","created_at","public_metrics","entities","geo","lang"]).flatten():
        parsed = {}
        parsed['id'] = retweet.id
        parsed['author_id'] = retweet.author_id
        parsed['tweet_id'] = tweet.id
        parsed['created_at'] = str(retweet.created_at)
        parsed['text'] = retweet.text
        parsed['public_metrics'] = retweet.public_metrics
        parsed['entities'] = retweet.entities
        parsed['geo'] = retweet.geo
        parsed['lang'] = retweet.lang
        #TODO Send parsed dict to mongo db (retweet)
        file_retweets.write(json.dumps(parsed, indent=4))
        rtw_count += 1
    
    logging.info("[*] Retweets collected: " + str(rtw_count))

    return rtw_count


#
#   This method collects all the directly replies of a tweet
#   The output is saved in output/replies.json
#
def get_replies(tweet_id):

    logging.info("Collecting replies of " + str(tweet_id) + "...")

    rpl_count = 0

    for reply in tw.Paginator(client.search_recent_tweets,query="conversation_id:{}".format(tweet_id),tweet_fields=["author_id","created_at","public_metrics","entities","geo","lang","referenced_tweets"]).flatten():
        refs = []
        answer_to_tweet = False

        # This condition filter only the direct replies to the tweet (excluding the replies of replies)

        for el in reply.referenced_tweets:
            if(el.id == tweet_id):
                answer_to_tweet = True
            rt = {}
            rt['type'] = el.type
            rt['id'] = el.id
            refs.append(rt)

        if(answer_to_tweet):
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
            #TODO Send parsed dict to mongo db (reply)
            file_replies.write(json.dumps(parsed, indent=4))
            rpl_count += 1

    logging.info("[*] Replies collected: " + str(rpl_count))

    return rpl_count

if __name__ == "__main__":


    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Profile", style="dim")
    table.add_column("Tweet")
    table.add_column("Retweets collected", justify="right")
    table.add_column("Replies collected", justify="right")

    console = Console()

    start_time = time.time()

    #
    #   Settings
    #

    load_dotenv()

    logger = logging.getLogger('spam_application')

    logging.basicConfig(filename='.appdata/app.log', level=logging.INFO, filemode='w', format="[" + str(datetime.datetime.now(timezone.utc).replace(microsecond=0)) + "] "+'%(name)s - %(levelname)s - %(message)s')

    file_state = open(".appdata/state.json", "w")
    file_tweets = open("output/tweets.json", "w")
    file_replies = open("output/replies.json", "w")
    file_retweets = open("output/retweets.json", "w")

    total_tw_count = 0

    total_rpl_count = 0

    total_rtw_count = 0

    client = connection_to_api() # API authentication
    
    profiles, keywords = load_filters() # Loading of profiles screen names and the keywords

    fromDate = datetime.datetime.now(timezone.utc).replace(microsecond=0) - datetime.timedelta(1)

    logging.info("Starting Tweets collector script")
    logging.info("Profiles loaded: " + str(len(profiles)))
    logging.info("Keywords loaded: " + str(len(keywords)))
    logging.info("Start date: " + fromDate.isoformat())

    #
    #   Algorithm
    #

    # Tweets collection

    for profile in profiles:

        # Get the user_id of a screen name

        user_id = client.get_user(username=profile)[0].id

        logging.info("Collecting tweets of " + profile + "...")

        for tweet in tw.Paginator(client.get_users_tweets,id = user_id, tweet_fields=["author_id","created_at","public_metrics","entities","geo","lang"], exclude=["retweets", "replies"],start_time=fromDate.isoformat()).flatten():
            for keyword in keywords:
                if f' {keyword.casefold()} ' in f' {tweet.text.casefold()} ':
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
                    #TODO Send parsed dict to mongo db (tweet)

                    logging.info("[*] Tweet " + str(tweet.id) +  " collected")

                    replies_cnt = get_replies(tweet.id)

                    retweets_cnt = get_retweets(profile,tweet.id)

                    os.system('cls' if os.name == 'nt' else 'clear')

                    text_to_print = ''

                    if(len(tweet.text)<25):
                        text_to_print = tweet.text
                    else:
                        text_to_print = tweet.text[:25] + "..."

                    table.add_row(
                        profile,
                        str(text_to_print.replace('\n', ' ')),
                        str(retweets_cnt),
                        str(replies_cnt),
                    )

                    console.print(table)

                    total_tw_count += 1
                    total_rpl_count += replies_cnt
                    total_rtw_count += retweets_cnt

    end_time = time.time()

    exec_time = time.gmtime(end_time-start_time)
    exec_time = time.strftime("%H:%M:%S",exec_time)

    logging.info("Collection finished")
    logging.info("[*] Tweets collected: " + str(total_tw_count))
    logging.info("[*] Retweets collected: " + str(total_rtw_count))
    logging.info("[*] Replies collected: " + str(total_rpl_count))
    logging.info("Execution time: " + str(exec_time))
    print("\n")
    print("Tweets collected: ",total_tw_count)
    print("Retweets collected: ",total_rtw_count)
    print("Replies collected: ",total_rpl_count)
    print("Execution time: " + str(exec_time))
    print("\n")
    state = {}
    state['last_run'] = str(datetime.datetime.now(timezone.utc).replace(microsecond=0))
    state['total_tweets_collected'] = total_tw_count
    state['total_replies_collected'] = total_rpl_count
    state['total_retweets_collected'] = total_rtw_count
    state['execution_time'] = exec_time
    file_state.write(json.dumps(state, indent=4))

    file_state.close()
    file_tweets.close()
    file_replies.close()
    file_retweets.close()