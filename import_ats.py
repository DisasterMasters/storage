import re
import datetime
from email.utils import format_datetime
import contextlib
import sys

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

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <input collection> <output collection>", file = sys.stderr)
        exit(-1)

    with contextlib.closing(pymongo.MongoClient()) as conn:
        coll_in = conn["twitter"][sys.argv[1]]
        coll_out = conn["twitter"][sys.argv[2]]

        coll_out.create_index([('id', pymongo.HASHED)], name = 'id_index')
        coll_out.create_index([('id', pymongo.ASCENDING)], name = 'id_ordered_index')
        coll_out.create_index([('screen_name', pymongo.HASHED)], name = 'screen_name_index')
        coll_out.create_index([('description', pymongo.TEXT)], name = 'description_index')

        visited = set()

        for r_in in coll_in.find():
            try:
                usernames = [r["user"]] + r["entities"]["user_mentions"]

                if "extended_tweet" in r:
                    uids += r["extended_tweet"]["entities"]["user_mentions"]

                usernames = (u["id"] for u in uids)
            except KeyError:
                usernames = (match.group()[1:] for match in re.finditer(r"@[A-Za-z0-9_]{1,15}", r["text"]))

            users = []

            for u in usernames:
                if u in visited:
                    continue

                visited.add(u)

                try:
                    user = api.get_user(
                        u,
                        tweet_mode = "extended",
                        include_entities = True,
                        monitor_rate_limit = True,
                        wait_on_rate_limit = True
                    )

                    timestamp = format_datetime(datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc))

                except tweepy.TweepError:
                    continue

                user["retrieved_at"] = timestamp
                users.append(user)

            coll_out.insert_many(users, ordered = False)

        # Remove duplicates
        dups = []
        ids = set()

        for r in coll_out.find(projection = ["id"]):
            if r['id'] in ids:
                dups.append(r['_id'])

            ids.add(r['id'])

        coll_out.delete_many({'_id': {'$in': dups}})
