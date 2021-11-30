import tweepy as tw
import json
from dotenv import load_dotenv
import os
from datetime import timezone
import datetime


#
#   Method for establish a connection whit Twitter API
#
def connection_to_api():
    auth = tw.OAuthHandler(os.getenv("API_KEY"), os.getenv("API_KEY_SECRET"))
    api = tw.API(auth, wait_on_rate_limit=True)
    return api

#
#   Create a twitter member list (if is needed)
#
def list_creation(api, profiles):
    list = api.create_list("profiles")
    for profile in profiles:
          api.add_list_member(list_id = list.id, screen_name = profile)
    return list.id


#
#   Load profiles screen name from profiles.txt and words from words.txt
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

    load_dotenv()

    api = connection_to_api()

    profiles, keywords = load_filters()

    keywords_query = "("

    for keyword,i in zip(keywords, range(len(keywords))):
        keywords_query += keyword
        if(i<len(keywords)-1):
            keywords_query += " OR "
    keywords_query += ")"

    fromDate = datetime.date.today() - datetime.timedelta(1)

    file_results = open("output/out.txt", "w")

    for profile in profiles:
        query = keywords_query + " from: " + profile
        for status in tw.Cursor(api.search_30_day,label=os.getenv("DEV_ENVIRONMENT") ,fromDate= fromDate.strftime("%Y%m%d%H%M"), query= query).items():
            file_results.write(json.dumps(status._json, indent=4))
            file_results.write("\n\n\n ---------------- \n\n\n")