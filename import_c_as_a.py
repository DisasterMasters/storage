import re
import copy
import csv
import sys
import threading
import os
import datetime
import itertools

from common import *

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

def read_bsv(filename, coll, coll_mut):
    print(filename + ": Starting")

    api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

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
        #(r"[KkWw][A-Za-z]{3}[^/]*.txt\Z", "media"),
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

                timestamp = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

            except tweepy.TweepError:
                continue

            status = adddates(statusconv(status), timestamp)
            status["original_file"] = filename
            status["original_line"] = lineno
            status["categories"] = categories

            coll.insert_one(status)

    print(filename + ": Done")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <DATA dir> <output collection>", file = sys.stderr)
        exit(-1)

    with openconn() as conn, opencoll(conn, sys.argv[2]) as coll:
        for dirpath, _, filenames in os.walk(sys.argv[1]):
            for filename in filenames:
                if "_WCOORDS.txt" in filename:
                    read_bsv(os.path.join(dirpath, filename), coll, None)
