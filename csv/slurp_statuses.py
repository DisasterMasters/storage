import datetime
from email.utils import parsedate_to_datetime
import mmap
import os
import re
import sys
import threading

import tweepy

from ..common import *

def read_csv(filename, api, api_mut, coll, coll_mut):
    fileno = os.open(filename, O_RDONLY)

    records = []
    dups = set()

    # "Python is a very good language because of its succinctness, you
    # definitely WON'T need four try/with blocks on top of one another"
    try:
        with mmap.mmap(fileno, 0, access = mmap.ACCESS_READ) as mm:
            for match in re.finditer(rb"[0-9]{15,}", mm):
                id = int(match.group().decode())

                if id in dups:
                    continue

                dups.add(id)

                try:
                    with api_mut:
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

                r = adddates(statusconv(r), retrieved_at)
                r["original_file"] = filename

                records.append(r)
    finally:
        os.close(fileno)

    if records:
        with coll_mut:
            coll.insert_many(records, ordered = False)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: %s <DATA dir> <output collection>", file = sys.stderr)
        exit(-1)

    with openconn() as conn, opencoll(conn, sys.argv[-1]) as coll:
        api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

        api_mut = threading.Lock()
        coll_mut = threading.Lock()
        pool = []

        for arg in sys.argv[1:-1]:
            if os.path.isdir(arg):
                for dirpath, _, filenames in os.walk(arg):
                    for filename in filenames:
                        pool.append(threading.Thread(
                            target = read_csv,
                            args = (os.path.join(dirpath, filename), api, api_mut, coll, coll_mut)
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
