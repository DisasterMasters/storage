import datetime
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
ID_REGEX = re.compile(rb"[0-9]{15,}")
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
                id_list = [int(match.group().decode()) for match in ID_REGEX.finditer(mm)]
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

    statuses = []
    failures = []

    # Load cached objects, if they exist
    try:
        with open(sys.argv[1] + ".pkl", "rb") as fd:
            while True:
                try:
                    cached_statuses, cached_failures = pickle.load(self.fd)
                except EOFError:
                    break

                statuses.extend(cached_statuses)
                failures.extend(cached_failures)
    except FileNotFoundError:
        pass

    # Remove statuses that have already been checked
    for status in statuses:
        id_set.discard(status["id"])

    for failure in failures:
        id_set.discard(failure)

    for id, doflush in zip(id_set, itertools.count()):
        try:
            r = api.get_status(
                id,
                tweet_mode = "extended",
                include_entities = True,
                monitor_rate_limit = True,
                wait_on_rate_limit = True
            )

            retrieved_at = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

        except tweepy.TweepError:
            failures.append(id)
            continue

        statuses.append(adddates(statusconv(r), retrieved_at))

        # Every so often, save the statuses that we have to a file
        if doflush % FLUSH_FREQ == 0:
            with open(fname, "ab") as fd:
                pickle.dump((statuses[-FLUSH_FREQ:], failures[-FLUSH_FREQ:]), fd)

    # Add the statuses to the collection
    with opendb() as db, opencoll(db, sys.argv[-1]) as coll:
        coll.insert_many(statuses, ordered = False)
