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

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <pos.txt> <neg.txt>" % sys.argv[0], file = sys.stderr)
        exit(-1)

    with contextlib.closing(pymongo.MongoClient()) as conn:
        coll = conn['twitter']['labeledMFilmReviews']

        coll.create_index([('text', pymongo.TEXT)], name = 'search_index', default_language = 'english')

        with open(sys.argv[1], "r") as fd:
            pos = [{"text": ln.strip(), "sentiment": "positive"} for ln in fd]

        with open(sys.argv[2], "r") as fd:
            neg = [{"text": ln.strip(), "sentiment": "negative"} for ln in fd]

        coll.insert_many(pos + neg, ordered = False)

        # Delete duplicate tweets
        dups = []
        ids = set()

        for r in coll.find():
            if r['text'] in ids:
                dups.append(r['_id'])

            ids.add(r['text'])

        coll_labeled.delete_many({'_id': {'$in': dups}})
