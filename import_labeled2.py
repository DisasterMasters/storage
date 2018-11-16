import csv
import copy
import datetime
import contextlib
import sys
import itertools
import pickle
from email.utils import format_datetime
from urllib.request import urlopen
from urllib.parse import urlencode

import nltk
import pymongo
from fuzzywuzzy import process
import tweepy

TWITTER_AUTH = tweepy.OAuthHandler(
    "ZFVyefAyg58PTdG7m8Mpe7cze",
    "KyWRZ9QkiC2MiscQ7aGpl5K2lbcR3pHYFTs7SCVIyxMlVfGjw0"
)
TWITTER_AUTH.set_access_token(
    "1041697847538638848-8J81uZBO1tMPvGHYXeVSngKuUz7Cyh",
    "jGNOVDxllHhO57EaN2FVejiR7crpENStbZ7bHqwv2tYDU"
)

def extended_to_compat(status, status_permalink = None):
    r = copy.deepcopy(status)

    full_text = r["full_text"]
    entities = r["entities"]

    r["extended_tweet"] = {
        "full_text": r["full_text"],
        "display_text_range": r["display_text_range"],
        "entities": r["entities"]
    }

    del r["full_text"]
    del r["display_text_range"]

    if "extended_entities" in r:
        r["extended_tweet"]["extended_entities"] = r["extended_entities"]
        del r["extended_entities"]

    if len(full_text) > 140:
        r["truncated"] = True

        if status_permalink is None:
            long_url = "https://twitter.com/tweet/web/status/" + r["id_str"]

            # Use TinyURL to shorten link to tweet
            with urlopen('http://tinyurl.com/api-create.php?' + urlencode({'url': long_url})) as response:
                short_url = response.read().decode()

            status_permalink = {
                "url": short_url,
                "expanded_url": long_url,
                "display_url": "twitter.com/tweet/web/status/\u2026",
                "indices": [140 - len(short_url), 140]
            }
        else:
            short_url = status_permalink["url"]
            status_permalink["indices"] = [140 - len(short_url), 140]

        r["text"] = full_text[:(138 - len(short_url))] + "\u2026 " + short_url

        r["entities"] = {
            "hashtags": [],
            "symbols": [],
            "user_mentions": [],
            "urls": [status_permalink]
        }

        for k in r["entities"].keys():
            for v in entities[k]:
                if v["indices"][1] <= 138 - len(short_url):
                    r["entities"][k].append(v)

    else:
        r["text"] = full_text
        r["entities"] = {k: entities[k] for k in ("hashtags", "symbols", "user_mentions", "urls")}

    if "quoted_status" in r:
        if "quoted_status_permalink" in r:
            quoted_status_permalink = r["quoted_status_permalink"]
            del r["quoted_status_permalink"]
        else:
            quoted_status_permalink = None

        r["quoted_status"] = extended_to_compat(r["quoted_status"], quoted_status_permalink)

    return r

if __name__ == "__main__":
    csvin = csv.DictReader(sys.stdin, quoting = csv.QUOTE_ALL)

    tweets = list(csvin)

    csvout = csv.DictWriter(sys.stdout, fieldnames = csvin.fieldnames,  quoting = csv.QUOTE_ALL)

    with contextlib.closing(pymongo.MongoClient()) as conn:
        coll = conn['twitter']['rawCIrma']
        api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

        def relookup(tweet):
            def p(a):
                print("\"" + tweet["TweetText"] + "\" -> " + a, file = sys.stderr)

            try:
                new_tweet = api.get_status(
                    int(tweet["TweetId"]),
                    tweet_mode = "extended",
                    include_entities = True,
                    monitor_rate_limit = True,
                    wait_on_rate_limit = True
                )

                timestamp = format_datetime(datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc))

            except KeyError:
                p("Failed (no status ID)")
                return None

            except tweepy.TweepError as e:
                p("Failed (Tweepy error: %r)" % e)
                return None

            new_tweet = extended_to_compat(new_tweet)
            new_tweet["retrieved_at"] = timestamp
            new_tweet["sentiment"] = tweet["Sentiment"]

            p('"' + new_tweet["text"] + '"')

            return new_tweet

        tweets = list(zip(tweets, map(relookup, tweets)))

        successes = [new_tweet for _, new_tweet in tweets if new_tweet is not None]
        failures = [tweet for tweet, new_tweet in tweets if new_tweet is None]

        coll_labeled = conn['twitter']['labeledATechCorporations']

        coll_labeled.create_index([('id', pymongo.HASHED)], name = 'id_index')
        coll_labeled.create_index([('id', pymongo.ASCENDING)], name = 'id_ordered_index')
        coll_labeled.create_index([('text', pymongo.TEXT)], name = 'search_index', default_language = 'english')

        coll_labeled.insert_many(successes, ordered = False)

        # Delete duplicate tweets
        dups = []
        ids = set()

        for r in coll_labeled.find(projection = ["id"]):
            if r['id'] in ids:
                dups.append(r['_id'])

            ids.add(r['id'])

        coll_labeled.delete_many({'_id': {'$in': dups}})

    csvout.writeheader()
    for f in failures:
        csvout.writerow(f)
