import requests
import io
import json

from tweepy.streaming import StreamListener 
from tweepy import OAuthHandler, Stream

from datetime import datetime, date, time

import pandas as pd
import numpy as np

"""
the app and keys for Twitter authentication are stored in a json file
using the format

{
   "my_app_name": {
      "api_key": "...",
      "api_secret": "...",
      "access_token": "...",
      "access_token_secret": "..."
   }
}

Naturally, one has to replace `my_app_name` with the appropriate app name 
from their own Twitter developer account, as well as the keys and tokens.
"""

keys = json.load(open('./twitter-api-apps-and-keys.json'))

# file name of tweets dataset
# at this stage, derived from current date and time
# https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
# REFACTORING: should probably use os.path.join
timestamp_str = datetime.now().strftime('%Y%m%d-%H%M%S')
dataset_fname = "./" + timestamp_str + "-" + "top_emojis.json"

# log file name
log_fname = timestamp_str + "-" + "....log"


# change `my_app_name`...
app_keys = keys['my_app_name']
auth = OAuthHandler(app_keys['api_key'], app_keys['api_secret'])
auth.set_access_token(
    app_keys['access_token'], app_keys['access_token_secret']
)


# parameters for streaming, including language!
# strings provided are supposed to be invididual words. It won't find 
# substrings.
# 
# https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters
# 
# do these operators work with streaming or just with search API?
# https://developer.twitter.com/en/docs/tweets/rules-and-filtering/overview/standard-operators.html

# for example...
tracking = [ 'brexit', 'EU referendum' ]


# what I'd like to project out of the tweets...
keys_of_interest = [
    "id_str",
    "in_reply_to_status_id",
    "quoted_status_id_str",
    "is_quote_status",
    "created_at",
    "text"
]
keys_of_interest_user = [
    "created_at",
    "description",
    "favourites_count",
    "followers_count",
    "friends_count",
    "lang",
    "name",
    "screen_name",
    "id_str",
    "statuses_count",
#     "time_zone",
    "verified"
]
def proj_attr_interest_tweet(tweet):
    return dict(
        { 
            k: tweet[k] 
            for k in keys_of_interest if k in tweet.keys() 
        }, 
        user={ 
            k: tweet['user'][k] 
            for k in keys_of_interest_user if k in tweet['user'].keys() 
        }
    )



# how long to wait on a rate limit issue 
rate_limit_status_code = 420
# https://docs.python.org/2/library/time.html
# sleep time in seconds, but I am expressing it in minutes
rate_limit_wait_min = 30


class Listener(StreamListener):
    def __init__(self, f_out):
        self.f_out = f_out
        super(Listener, self).__init__()

    # Data Wrangling with Python suggested overwriting `on_data` but, after
    # reading the `tweepy` documentation and source code, I realised that it is
    # best to use `on_status`

    # def on_data(self, data):
    #   json_data = json.loads(data)
    #   print(data) 
    #   self.f_out.write(unicode(json.dumps(json_data, ensure_ascii=False, sort_keys=True)))
    #   # self.f_out.write(data)
    #   self.f_out.write(u'\n')
    #   return True

    def on_status(self, data):
        # the data seems to have already been converted from JSON
        tweet = data._json
        tweet_proj = proj_attr_interest_tweet(tweet)

        self.f_out.write(
            json.dumps(tweet_proj, ensure_ascii=False, sort_keys=True)
        )
        self.f_out.write(u'\n')

        return True

    def on_error(self, status_code):
        if status_code == rate_limit_status_code:
            #returning False in on_data disconnects the stream
            time.sleep(rate_limit_wait_min*60)
            return True

# print(tracking)

with io.open(dataset_fname, "a", encoding="utf-8") as f_out:
    # print("Streaming...", end=" ")
    print("Streaming...")
    stream = Stream(auth=auth, listener=Listener(f_out))
    stream.filter(track=tracking, languages=['en'])


