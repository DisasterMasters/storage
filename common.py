import contextlib
import copy
import datetime
from email.utils import format_datetime
import socket
from urllib.request import urlopen
from urllib.parse import urlencode

import pymongo
import tweepy

TWITTER_AUTH = tweepy.OAuthHandler(
    "ZFVyefAyg58PTdG7m8Mpe7cze",
    "KyWRZ9QkiC2MiscQ7aGpl5K2lbcR3pHYFTs7SCVIyxMlVfGjw0"
)
TWITTER_AUTH.set_access_token(
    "1041697847538638848-8J81uZBO1tMPvGHYXeVSngKuUz7Cyh",
    "jGNOVDxllHhO57EaN2FVejiR7crpENStbZ7bHqwv2tYDU"
)

MONGODB_HOST = "da1.eecs.utk.edu" if socket.gethostname() == "75f7e392a7ec" else "localhost"

'''
# Open a default connection
def openconn():
    return contextlib.closing(pymongo.MongoClient("da1.eecs.utk.edu" if socket.gethostname() == "75f7e392a7ec" else "localhost"))

# Open a default collection (setting up indices and removing duplicates)
@contextlib.contextmanager
def opencoll(conn, collname, dbname = "twitter"):
    coll = conn[dbname][collname]

    # Set up indices
    coll.create_indices([
        pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
        pymongo.IndexModel([('user.id', pymongo.HASHED)], name = 'user_id_index'),
        pymongo.IndexModel([('user.screen_name', pymongo.HASHED)], name = 'user_screen_name_index'),
        pymongo.IndexModel([('text', pymongo.TEXT)], name = 'search_index', default_language = 'english'),
        pymongo.IndexModel([('created_at', pymongo.ASCENDING)], name = 'created_at_index'),
        pymongo.IndexModel([('categories', pymongo.ASCENDING)], name = 'categories_index', sparse = True)
    ])

    yield coll

    # Remove duplicates
    dups = []
    ids = set()

    for r in coll.find(projection = ["id"]):
        if r['id'] in ids:
            dups.append(r['_id'])

        ids.add(r['id'])

    coll.delete_many({'_id': {'$in': dups}})
'''

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
