import tweepy as tw
import json
from dotenv import load_dotenv
import os


#
#   Method for establish a connection whit Twitter API
#
def connection_to_api():
    auth = tw.OAuthHandler(os.getenv("API_KEY"), os.getenv("API_KEY_SECRET"))
    auth.set_access_token(os.getenv("ACCESS_TOKEN"), os.getenv("ACCESS_TOKEN_SECRET"))
    api = tw.API(auth, wait_on_rate_limit=True)
    client = tw.Client(bearer_token= os.getenv("BAERER_TOKEN"))
    return api, client

#
#   Create a twitter member list (if is needed)
#
def list_creation(api, profiles):
    list = api.create_list("profiles")
    for profile in profiles:
          api.add_list_member(list_id = list.id, screen_name = profile)
    return list.id


#
#   Load profiles screen name from profiles.txt
#
def load_profiles():
    profiles = []
    file = open("profiles.txt", "r")
    for line in file:
        profiles.append(line)
    return profiles



if __name__ == "__main__":

    load_dotenv()

    api, client = connection_to_api()

    profiles = load_profiles()

    #TODO api.search_full_archive(label="development", query = "")

    for profile in profiles:
        for status in tw.Cursor(api.user_timeline, screen_name = profile, exclude_replies = True, include_rts = False, tweet_mode="extended").items(2):
            print(status.full_text)
            print()