from email.utils import format_datetime
import queue
import signal
import sys
import threading

import tweepy

from common import *
from oldtweets import OldKeywordThread, OldUsernameThread
from newtweets import NewKeywordThread, NewUsernameThread

def print_status(status):
    print("\"%s\" -- @%s, %s (retrieved %s)" % (
        status["text"],
        status["user"]["screen_name"],
        format_datetime(status["created_at"]),
        format_datetime(status["retrieved_at"])
    ))

def put_statuses_into_collection(coll, qu):
    while True:
        status = qu.get()

        if status is signal.SIGINT:
            print("SIGINT received")
            break
        elif type(status) is list and type(status[0]) is dict:
            # For debugging
            print("\033[1m\033[31mGot some old tweets\033[0m")

            for r in status:
                print_status(r)

            coll.insert_many(status, ordered = False)
        elif type(status) is dict:
            print_status(status)

            coll.insert_one(status)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: " + sys.argv[0] + "<config_file.py>", file = sys.stderr)
        exit(-1)

    try:
        qu = queue.SimpleQueue()
    except AttributeError: # Fix for Python <3.7
        qu = queue.Queue()

    ev = threading.Event()

    def sigint(sig, frame):
        ev.set()
        qu.put(signal.SIGINT)

    signal.signal(signal.SIGINT, sigint)

    pool = []
    opts = {}
    with open(sys.argv[1], "r") as fd:
        exec(fd.read(), opts)

    old_keywords = opts["KEYWORDS"] + opts["OLD_KEYWORDS"]
    new_keywords = opts["KEYWORDS"] + opts["NEW_KEYWORDS"]

    old_usernames = opts["USERNAMES"] + opts["OLD_USERNAMES"]
    new_usernames = opts["USERNAMES"] + opts["NEW_USERNAMES"]

    old_locations = opts["LOCATIONS"] + opts["OLD_LOCATIONS"]
    new_locations = opts["LOCATIONS"] + opts["NEW_LOCATIONS"]

    if old_keywords:
        pool.append(OldKeywordThread(old_keywords, qu, ev))
        pool[-1].start()

    if new_keywords:
        pool.append(NewKeywordThread(new_keywords, qu, ev))
        pool[-1].start()

    if old_usernames:
        pool.append(OldUsernameThread(old_usernames, qu, ev))
        pool[-1].start()

    if new_usernames:
        pool.append(NewUsernameThread(new_usernames, qu, ev))
        pool[-1].start()

    if old_locations:
        pool.append(OldLocationThread(old_usernames, qu, ev))
        pool[-1].start()

    if new_locations:
        pool.append(NewLocationThread(new_usernames, qu, ev))
        pool[-1].start()

    with opendb() as db, opencoll(db, opts["COLLNAME"]) as coll:
        put_statuses_into_collection(coll, qu)

        for thrd in pool:
            thrd.join()
