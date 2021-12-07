#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 01:38:02 2021

@author: Ayca
"""

import pandas as pd
import tweepy
import json

consumer_key= ''
consumer_secret= ''


client = tweepy.Client(bearer_token='')


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  
auth.set_access_token('', '')
  
api = tweepy.API(auth)

query = 'from:AycaBegum -is:retweet'

tweets = client.search_all_tweets(query=query, start_time='2021-01-01T00:00:00Z',end_time='2021-12-06T00:00:00Z' , tweet_fields=['context_annotations', 'created_at'], max_results=100)
tweets_data = tweets.data
for tweet in tweepy.Cursor(api.user_timeline, screen_name="aycabegum", tweet_mode="extended"). items(1): 
    en_json=json.dumps(tweet._json, indent=2)
    en_dict= json.loads(en_json) 
    print(en_json)
    print("este es el texto sacado del diccionario: \n",en_dict['full_text'])
 

for i in list(en_dict.keys()):
  print("key: {}, type: {}".format(i,type(en_dict[i])))
  
print(en_dict['user']['entities'])

del en_dict['user']['entities']
print(en_dict['user'])

print(en_dict['entities'])

tmp = en_dict['user']
print(tmp)

user_df = pd.DataFrame.from_dict(tmp,orient='index')
print(user_df)

entities_df = pd.DataFrame.from_dict(en_dict['entities'],orient='index')
print(entities_df)

del en_dict['user']
del en_dict['entities']
print(en_dict)

main_df = pd.DataFrame.from_dict(en_dict,orient='index')
print(main_df)