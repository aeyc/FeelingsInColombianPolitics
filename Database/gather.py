#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 02:00:20 2021

@author: Ayca
"""
import pandas as pd
from pymongo import MongoClient

try:
    #modify the <password> part
    cluster = MongoClient("mongodb+srv://colombia:<password>@colombia1.vp4wl.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    print("Connected") 
    db = cluster["testDB"]
    collection = db["testC"]
    cursor = collection.find()
    if cursor.count() == 0:
        print("Empty cursor") #the desired info cannot be gathered
    else:
        
        cursor = list(cursor)
        df_gathered = pd.DataFrame(cursor)

except:   
    print("Could not connect to MongoDB")