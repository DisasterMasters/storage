import re
import copy
import csv
import sys
import threading
import os
import datetime
import itertools
from email.utils import format_datetime
from urllib.request import urlopen
from urllib.parse import urlencode
import contextlib

import tweepy
import pymongo

TWITTER_AUTH = tweepy.OAuthHandler(
    "ZFVyefAyg58PTdG7m8Mpe7cze",
    "KyWRZ9QkiC2MiscQ7aGpl5K2lbcR3pHYFTs7SCVIyxMlVfGjw0"
)
TWITTER_AUTH.set_access_token(
    "1041697847538638848-8J81uZBO1tMPvGHYXeVSngKuUz7Cyh",
    "jGNOVDxllHhO57EaN2FVejiR7crpENStbZ7bHqwv2tYDU"
)

# Convert tweets obtained with extended REST API to a format similar to the
# compatibility mode used by the streaming API
def statusconv(status, status_permalink = None):
    r = copy.deepcopy(status)

    if "extended_tweet" in r:
        return r

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

        r["quoted_status"] = statusconv(r["quoted_status"], quoted_status_permalink)

    return r

def read_bsv(filename, coll, api):
    print(filename + ": Starting")

    category_list = [
        (r"gov_data/", "gov"),
        (r"utility_data/", "utility"),
        (r"media_data/", "media"),
        (r"nonprofit_data/", "nonprofit"),
        (r"Environmental Groups/", "nonprofit"),
        (r"First Responders and Gov't/", "gov"),
        (r"Individuals/", "private"),
        (r"Insurance/", "private"),
        (r"Local Media/", "media"),
        (r"National Media/", "media"),
        (r"Nonprofits/", "nonprofit"),
        (r"Utilities/", "utility"),
        (r"[KkWw][A-Za-z]{3}[^/]*.txt\Z", "media"),
        (r"City[Oo]f[A-Za-z]+.txt\Z", "gov"),
        (r"County.txt\Z", "gov")
    ]

    categories = set()

    for regex, category in category_list:
        if re.search(regex, filename) is not None:
            categories.add(category)

    categories = list(categories)

    with open(filename, "r") as fd:
        matches = itertools.chain.from_iterable(zip(
            itertools.repeat(i + 1),
            re.finditer(r"[0-9]{15,}", ln)
        ) for i, ln in enumerate(fd))

        for lineno, match in matches:
            try:
                status = api.get_status(
                    int(match.group()),
                    tweet_mode = "extended",
                    include_entities = True,
                    monitor_rate_limit = True,
                    wait_on_rate_limit = True
                )

                timestamp = format_datetime(datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc))

            except tweepy.TweepError:
                continue

            status["original_file"] = filename
            status["original_line"] = lineno
            status["retrieved_at"] = timestamp
            status["categories"] = categories

            coll.insert_one(status)

    print(filename + ": Done")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <DATA dir> <output collection>", file = sys.stderr)
        exit(-1)

    with contextlib.closing(pymongo.MongoClient()) as conn:
        coll = conn["twitter"][sys.argv[2]]
        api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

        # Set up indices
        coll.create_index([('id', pymongo.HASHED)], name = 'id_index')
        coll.create_index([('id', pymongo.ASCENDING)], name = 'id_ordered_index')
        coll.create_index([('text', pymongo.TEXT)], name = 'search_index', default_language = 'english')

        for dirpath, _, filenames in os.walk(sys.argv[1]):
            for filename in filenames:
                if "_WCOORDS.txt" in filename:
                    read_bsv(os.path.join(dirpath, filename), coll, api)

        # Remove duplicates
        dups = []
        ids = set()

        for r in coll.find(projection = ["id"]):
            if r['id'] in ids:
                dups.append(r['_id'])

            ids.add(r['id'])

        coll.delete_many({'_id': {'$in': dups}})
