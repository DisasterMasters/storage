import csv
import contextlib
import itertools
import sys

import tweepy

from common import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage:", sys.argv[0], "<config file.py>", file = sys.stderr)
        exit(-1)

    api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    opts = {}
    with open(sys.argv[1], "r") as fd:
        exec(fd.read(), opts)

    with contextlib.ExitStack() as exitstack:
        conn = exitstack.enter_context(openconn())
        coll = exitstack.enter_context(opencoll(conn, opts["COLLNAME"]))

        colls_in = [conn["twitter"][collname] for collname in opts["COLLECTIONS_STATUSES_A"]]

        id_search = lambda id: id_search(colls_in, id)
        text_search = lambda text: text_search(colls_in, text)

        sources = []

        for collname in opts["COLLECTIONS_STATUSES_C"]:
            cursor = conn["twitter"][collname].find(no_cursor_timeout = True)
            exitstack.enter_context(contextlib.closing(cursor))

            sources.append(cursor)

        for filename in opts["FILES_STATUSES_C"]:
            fd = exitstack.enter_context(open(filename, "r", newline = ""))

            dialect = csv.Sniffer().sniff(fd.read(4096))
            fd.seek(0)

            sources.append(csv.DictReader(fd, dialect = dialect))

        for labeled_datum in itertools.chain.from_iterable(sources):
            if opts["ID_FIELD"] is None:
                r = text_search(labeled_datum[opts["TEXT_FIELD"]])

                if r is None:
                    continue
            else:
                id = int(labeled_datum[opts["ID_FIELD"]])

                r = id_search(id)

                if r is None:
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

                    r = adddates(statusconv(r), retrieved_at)

            for k in opts["FIELDS_KEEP"]:
                r[k] = labeled_datum[k]

            conn.insert_one(r)
            # TODO

