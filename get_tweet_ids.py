import csv
import datetime
import contextlib
import sys
import itertools
import pickle
from email.utils import format_datetime

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

labelled_tweets = []

with contextlib.closing(pymongo.MongoClient()) as conn:
    coll_fl = conn['twitter']['rawCIrma']

    for row in csv.DictReader(sys.stdin, dialect = csv.unix_dialect):
        query = {"$text": {"$search": " ".join(nltk.tokenize.wordpunct_tokenize(row["Tweet"].strip().lower()))}}

        match_map = {r["text"]: r["_id"] for r in coll_fl.find(query, projection = ["text"])}
        best_match, _ = process.extractOne(row["Tweet"], list(match_map.keys()))

        print("%s -> %s (%r)" % (
            row["Tweet"],
            best_match,
            match_map[best_match]
        ))

        labelled_tweets.append((match_map[best_match], row["Manual Coding"]))

    coll_labelled = conn['twitter']['labelledAIrma']
    api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    coll_labelled.create_index([('id', pymongo.HASHED)], name = 'id_index')
    coll_labelled.create_index([('id', pymongo.ASCENDING)], name = 'id_ordered_index')
    coll_labelled.create_index([('text', pymongo.TEXT)], name = 'search_index', default_language = 'english')

    for _id, code in labelled_tweets:
        r = coll_fl.find_one({"_id": _id})

        try:
            id = r["ID"]
        except KeyError:
            print("Failed to locate tweet ID for " + repr(r), file = sys.stderr)
            continue

        r = api.get_status(
            id,
            tweet_mode = "extended",
            include_entities = True,
            monitor_rate_limit = True,
            wait_on_rate_limit = True
        )

        timestamp = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

        r = extended_to_compat(r)
        r["retrieved_at"] = format_datetime(timestamp)
        r["code"] = code

        coll_labelled.insert_one(r)

with open("supervised_data.p", "wb") as fd:
    pickle.dump(labelled_tweets, fd)
