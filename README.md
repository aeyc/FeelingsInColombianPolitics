# FeelingsInColombianPolitics

## How the TweetsCollector works?
The script collects all the tweets of some profiles which their screen name is defined in filters/profiles.txt.
It collects only those tweets that contain one or more keywords from filters/keywords.txt.
For each tweet collected it gets also the full list of replies (just the first level, not replies of replies) and the retweets with quotes.
After the execution all the data collected will be pushed to mongo DB.

## Instructions for running the TweetsCollector:
- insert your BAERER_TOKEN in .env.example file
- insert your MONGODB_PSW in .env.example file
- insert all the profiles you want to analyze in filters/profile.txt
- insert all the keywords you want to search in filters/keywords.txt
- rename .env.example file in .env
- run the script: python main.py