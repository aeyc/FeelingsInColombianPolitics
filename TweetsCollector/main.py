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
from pymongo import MongoClient

#Lists to be used in database
retweets_l = []
replies_l = []
tweets_l = []

#
#   Limit parameters for testing (if you are not using an academic account)
#

RESULTS_LIMIT = 10

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
    df = pd.DataFrame()
    
    for retweet in tw.Paginator(client.search_recent_tweets,query='url: "{}"'.format(url)  + " -is:reply -is:retweet is:quote", tweet_fields=["author_id","created_at","public_metrics","entities","geo","lang"]).flatten(RESULTS_LIMIT):
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
        file_retweets.write(json.dumps(parsed, indent=4))
        rtw_count += 1
        
        retweets_l.append(parsed)
    logging.info("[*] Retweets collected: " + str(rtw_count))

    return rtw_count


#
#   This method collects all the directly replies of a tweet
#   The output is saved in output/replies.json
#
def get_replies(tweet_id):

    logging.info("Collecting replies of " + str(tweet_id) + "...")

    rpl_count = 0

    for reply in tw.Paginator(client.search_recent_tweets,query="conversation_id:{}".format(tweet_id),tweet_fields=["author_id","created_at","public_metrics","entities","geo","lang","referenced_tweets"]).flatten(RESULTS_LIMIT):
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
            replies_l.append(parsed)
            file_replies.write(json.dumps(parsed, indent=4))
            rpl_count += 1

    logging.info("[*] Replies collected: " + str(rpl_count))

    return rpl_count

#
# Method to use flaaten json object gathered from the api
# Currently it drops the dictionary entities in one dictionary entity
#
def flatten_entities(lst):
    for i in lst:
        i['retweet_count'] = i['public_metrics']['retweet_count']
        i['reply_count'] = i['public_metrics']['reply_count']
        i['like_count'] = i['public_metrics']['like_count']
        i['quote_count'] = i['public_metrics']['quote_count']
        if i['entities'] is not None:
            if 'hashtags' in list(i['entities'].keys()):
                i['hashtags'] = i['entities']['hashtags']
            else:
                i['hashtags'] =[]
            
        del i['public_metrics']
        del i['entities']
    return lst


def insertDB(replies_df,retweets_df, tweets_df):

    cluster = MongoClient("mongodb+srv://colombia:"+os.getenv("MONGODB_PSW")+"@colombia1.vp4wl.mongodb.net/myFirstDatabase=true&w=majority")

    try: 
        #modify the <password> part
        db = cluster["RepliesDB"] #put your db name
        collection = db["RepliesC"] #put your collection name
        records = json.loads(replies_df.T.to_json()).values()
        collection.insert_many(records)
        logging.info("Replies Inserted")
        print("Replies Inserted")
    except Exception as e:
        logging.info(e)
        print(e)
    try: 
        db = cluster["RetweetsDB"]
        collection = db["RetweetsC"]
        records = json.loads(retweets_df.T.to_json()).values()
        collection.insert_many(records)
        logging.info("Retweets Inserted")
        print("Retweets Inserted")
        
    except Exception as e:   
        logging.info(e)
        print(e)
    try: 
        db = cluster["TweetsDB"]
        collection = db["TweetsC"]
        records = json.loads(tweets_df.T.to_json()).values()
        collection.insert_many(records)
        logging.info("Tweets Inserted")
        print("Tweets Inserted")
        
    except Exception as e:   
        logging.info(e)
        print(e)        


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

    logging.basicConfig(filename='.appdata/app.log', level=logging.INFO, filemode='w', format= '%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

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

        for tweet in tw.Paginator(client.get_users_tweets,id = user_id, tweet_fields=["author_id","created_at","public_metrics","entities","geo","lang"], exclude=["retweets", "replies"],start_time=fromDate.isoformat()).flatten(RESULTS_LIMIT):
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
                    tweets_l.append(parsed)
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
            
    replies_l = flatten_entities(replies_l)
    retweets_l = flatten_entities(retweets_l)
    tweets_l = flatten_entities(tweets_l)
    
    replies_df = pd.DataFrame(replies_l)
    retweets_df = pd.DataFrame(retweets_l)
    tweets_df = pd.DataFrame(tweets_l)
    
    insertDB(replies_df,retweets_df, tweets_df)