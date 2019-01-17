import datetime
import itertools
import mmap
import os
import pickle
import re
import sys
import threading

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    tqdm = lambda x: x

import tweepy

from common import *

def read_csv(filename, id_set, id_set_mut):
    print("%s: Starting" % filename)

    fileno = os.open(filename, os.O_RDONLY)

    try:
        with mmap.mmap(fileno, 0, access = mmap.ACCESS_READ) as mm:
            id_list = [int(match.group().decode()) for match in re.finditer(rb"[0-9]{15,}", mm)]
    finally:
        os.close(fileno)

    if id_list:
        with id_set_mut:
            id_set.update(id_list)

    print("%s: Done" % filename)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: %s <DATA dir> <output collection>", file = sys.stderr)
        exit(-1)

    api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    id_set = set()
    id_set_mut = threading.Lock()

    pool = []

    for arg in sys.argv[1:-1]:
        if os.path.isdir(arg):
            for dirpath, _, filenames in os.walk(arg):
                for filename in filenames:
                    if filename[filename.rfind('.'):] == ".txt":
                        pool.append(threading.Thread(
                            target = read_csv,
                            args = (os.path.join(dirpath, filename), id_set, id_set_mut)
                        ))

                        pool[-1].start()
        else:
            pool.append(threading.Thread(
                target = read_csv,
                args = (arg, api, api_mut, coll, coll_mut)
            ))

            pool[-1].start()

        # Wait for all threads to finish
    for thrd in pool:
        thrd.join()

    try:
        with open(sys.argv[1] + ".pkl", "rb") as fd:
            statuses = pickle.load(fd)
    except FileNotFoundError:
        statuses = []

    for status in statuses:
        id_set.discard(status["id"])

    print("Getting statuses...")

    for i, id in zip(itertools.count(), tqdm(id_set)):
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
            continue

        statuses.append(adddates(statusconv(r), retrieved_at))

        # Every so often, save the statuses that we have to a file
        if i % 450 == 0:
            with open(sys.argv[-1] + ".pkl", "wb") as fd:
                pickle.dump(statuses, fd)

    print("Adding statuses to collection %s..." % sys.argv[-1])

    with openconn() as conn, opencoll(conn, sys.argv[-1]) as coll:
        coll.insert_many(statuses)
