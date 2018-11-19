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

import pymongo
import tweepy

if __name__ == "__main__":
    tweets = {int(ln["TweetId"]): ln for ln in csv.DictReader(sys.stdin, quoting = csv.QUOTE_ALL)}

    with contextlib.closing(pymongo.MongoClient()) as conn:
        coll = conn['twitter']['labeledATechCorporations']

        for r in coll.find():
            del r["sentiment"]
            r["categories"] = [tweets[r["id"]]["Topic"], tweets[r["id"]]["Sentiment"]]

            coll.find_one_and_replace({"_id": r["_id"]}, r)

        coll.create_index([('categories', 1)], name = 'categories_index')
