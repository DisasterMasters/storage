import datetime
from email.utils import parsedate_to_datetime
import itertools
import mmap
import os
import pickle
import re
import sys
import threading

import tweepy

from common import *

FLUSH_FREQ = 450
ID_REGEX = re.compile(rb"@[A-Za-z0-9_]{1,15}")
CACHE_FILENAME = sys.argv[-1] + ".pkl"

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: %s <file/dir> [<file/dir> ...] <output collection>", file = sys.stderr)
        exit(-1)

    id_set = set()
    id_set_mut = threading.Lock()

    pool = []

    def thrd_f(filename):
        fileno = os.open(filename, os.O_RDONLY)

        try:
            with mmap.mmap(fileno, 0, access = mmap.ACCESS_READ) as mm:
                id_list = [int(match.group().decode()) for match in regex.finditer(mm)]
        finally:
            os.close(fileno)

        if id_list:
            with id_set_mut:
                id_set.update(id_list)

    # Spawn a thread for each file
    for arg in sys.argv[1:-1]:
        if os.path.isdir(arg):
            for dirpath, _, filenames in os.walk(arg):
                for filename in filenames:
                    if filename[filename.rfind('.'):] == ".txt":
                        pool.append(threading.Thread(
                            target = thrd_f,
                            args = (os.path.join(dirpath, filename))
                        ))

                        pool[-1].start()
        else:
            pool.append(threading.Thread(
                target = thrd_f,
                args = (arg)
            ))

            pool[-1].start()

    # Wait for all the threads to finish
    for thrd in pool:
        thrd.join()

    api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    users = []
    failures = []

    # Load cached objects, if they exist
    try:
        with open(sys.argv[1] + ".pkl", "rb") as fd:
            while True:
                try:
                    cached_users, cached_failures = pickle.load(self.fd)
                except EOFError:
                    break

                users.extend(cached_users)
                failures.extend(cached_failures)
    except FileNotFoundError:
        pass

    # Remove users that have already been checked
    for user in users:
        id_set.discard(user["id"])

    for failure in failures:
        id_set.discard(failure)

    for id, doflush in zip(id_set, itertools.count()):
        try:
            r = api.get_user(
                screen_name,
                tweet_mode = "extended",
                include_entities = True,
                monitor_rate_limit = True,
                wait_on_rate_limit = True
            )

            retrieved_at = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

        except tweepy.TweepError:
            failures.append(id)
            continue

        r["retrieved_at"] = retrieved_at
        r["created_at"] = parsedate_to_datetime(user["created_at"])

        if "status" in r:
            r["status"]["created_at"] = parsedate_to_datetime(r["status"]["created_at"])

        users.append(r)

        # Every so often, save the users that we have to a file
        if doflush % FLUSH_FREQ == 0:
            with open(fname, "ab") as fd:
                pickle.dump((users[-FLUSH_FREQ:], failures[-FLUSH_FREQ:]), fd)

    # Add the users to the collection
    with opendb() as db, opencoll(db, sys.argv[-1]) as coll:
        coll.insert_many(users, ordered = False)
