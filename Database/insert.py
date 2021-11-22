#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 01:57:24 2021

@author: Ayca
"""
import pandas as pd
from pymongo import MongoClient


#connect-insert data
#json_response['data'] is the data gathered from the api 
#add it to use the code
df = pd.DataFrame(json_response['data'])
try: 
    #modify the <password> part
    cluster = MongoClient("mongodb+srv://colombia:<password>@colombia1.vp4wl.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    print("Connected") 
    db = cluster["testDB"] #put your db name
    collection = db["testC"] #put your collection name
    records = json.loads(df.T.to_json()).values()
    collection.insert(records)
    
except:   
    print("Could not connect to MongoDB")