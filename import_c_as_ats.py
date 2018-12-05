import re
import copy
import csv
import sys
import threading
import os
import datetime
import itertools
import mmap
from email.utils import parsedate_to_datetime

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

    fileno = os.open(filename, os.O_RDONLY)
    usernames = set()

    # "Python is a very good language because of its succinctness"
    try:
        with mmap.mmap(fileno, 0, access = mmap.ACCESS_READ) as mm:
            for match in re.finditer(rb"@[A-Za-z0-9_]{1,15}", mm):
                username = match.group()[1:].decode()

                if username in usernames:
                    continue

                usernames.add(username)

                try:
                    user = api.get_user(
                        username,
                        tweet_mode = "extended",
                        include_entities = True,
                        monitor_rate_limit = True,
                        wait_on_rate_limit = True
                    )

                    timestamp = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

                except tweepy.TweepError:
                    continue

                user["retrieved_at"] = timestamp
                user["created_at"] = parsedate_to_datetime(user["created_at"])

                if "status" in user:
                    user["status"]["created_at"] = parsedate_to_datetime(user["status"]["created_at"])

                coll.insert_one(user)

    finally:
        os.close(fileno)

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
