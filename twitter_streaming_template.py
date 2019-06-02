import requests
import io
import json

from tweepy.streaming import StreamListener 
from tweepy import OAuthHandler, Stream

from datetime import datetime, date, time

import pandas as pd
import numpy as np

keys = json.load(open('./twitter-api-apps-and-keys.json'))
# keys['SocialLanguageStats']


# file name of tweets dataset
# later, it will be derived from the tracking terms (emojis, in this case)
# and current date and time
# https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
timestamp_str = datetime.now().strftime('%Y%m%d-%H%M%S')
dataset_fname = "./streamed_tweets/" + timestamp_str + "-" + "top_emojis.json"

# log file name
# later, it will be derived from the tracking terms and current date and time
log_fname = timestamp_str + "-" + "top_emojis.log"



app_keys = keys['NLP_Crawler_1']
auth = OAuthHandler(app_keys['api_key'], app_keys['api_secret'])
auth.set_access_token(
    app_keys['access_token'], app_keys['access_token_secret']
)


# based on 
# https://www.webpagefx.com/tools/emoji-cheat-sheet/
# http://emojitracker.com/
# top_emoji_names_symbol = [
#     (':joy:', '😂'),
#     (':heart:', '❤️'),
#     (':heart_eyes:', '😍'),
#     (':hearts:', '♥️'),  # issues here - black heart suit?
#     (':sob:', '😭'),
#     (':blush:', '😊'),
#     (':unamused:', '😒'),
#     (':two_hearts:', '💕'),
#     (':kissing_heart:', '😘'),
#     (':weary:', '😩'),
#     (':relaxed:', '☺️'),
#     (':ok_hand:', '👌'),
#     (':pensive:', '😔'),
#     (':grin:', '😁'),
#     (':smirk:', '😏'),
# ]

# emojis_to_track = [ t[1] for t in top_emoji_names_symbol ]

# I have now scraped and selected a subset of emojis from emojitracker
emojis_df = pd.read_csv('./selected_emoji_ranking_extract_emojitracker-fixed_char.csv')
# emojis_df.tail()
# emojis_df['fixed_emoji_char']
# list(emojis_df['fixed_emoji_char'])
emojis_to_track = list(emojis_df['fixed_emoji_char'])

# emojis_to_track = [ t[1] for t in top_emoji_names_symbol ]

# parameters for streaming, including language!
# strings provided are supposed to be invididual words. It won't find 
# substrings.
# Would it find syntactic emojis?
# 
# https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters
# 
# do these operators work with streaming or just with search API?
# https://developer.twitter.com/en/docs/tweets/rules-and-filtering/overview/standard-operators.html

# tracking = [ 'brexit', 'EU referendum' ]
# tracking = [ '#Brexit ' + e for e in emojis_to_track ]
# tracking = [ '#TrumpVisitsUK media' ]
tracking = emojis_to_track


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
        # I've seen this idiom somewhere, but I have no idea how this
        # unpacking operation works
        # **{ k: tweet['user'][k] for k in keys_of_interest_user if k in keys_of_interest_user }
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

  # the Data Wrangling with Python suggested overwriting `on_data` but, after
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
        json_data = data._json
        # print(json_data)
        # print(
        #     np.random.choice(emojis_df.fixed_emoji_char.values, 1)[0], 
        #     end=" "
        # )
        # tweet = json.loads(json_data)
        # the data seems to have already been converted from JSON?
        tweet = json_data
        tweet_proj = proj_attr_interest_tweet(tweet)

        self.f_out.write(
            # unicode(
                # json.dumps(json_data, ensure_ascii=False, sort_keys=True)
                json.dumps(tweet_proj, ensure_ascii=False, sort_keys=True)
            # )
        )
        self.f_out.write(u'\n')
        # return False
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


