from email.utils import format_datetime
import queue
import signal
import sys
import threading

import tweepy

from common import *
from oldtweets import OldKeywordThread, OldUsernameThread, OldLocationThread
from newtweets import NewKeywordThread, NewUsernameThread, NewLocationThread

def print_status(status):
    print("\"%s\" -- @%s, %s (retrieved %s)" % (
        status["text"],
        status["user"]["screen_name"],
        format_datetime(status["created_at"]),
        format_datetime(status["retrieved_at"])
    ))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: " + sys.argv[0] + "<config_file.py>", file = sys.stderr)
        exit(-1)

    try:
        qu = queue.SimpleQueue()
    except AttributeError: # Fix for Python <3.7
        qu = queue.Queue()

    ev = threading.Event()

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
        pool.append(OldLocationThread(old_locations, qu, ev))
        pool[-1].start()

    if new_locations:
        pool.append(NewLocationThread(new_locations, qu, ev))
        pool[-1].start()

    with opendb() as db:
        coll = db[opts["COLLNAME"]]

        try:
            while True:
                status = qu.get()

                if type(status) is list and type(status[0]) is dict:
                    # For debugging
                    print("\033[1m\033[31mGot some old tweets\033[0m")

                    for r in status:
                        print_status(r)

                    coll.insert_many(status, ordered = False)
                elif type(status) is dict:
                    print_status(status)

                    coll.insert_one(status)
        except KeyboardInterrupt:
            ev.set()

    for thrd in pool:
        thrd.join()
