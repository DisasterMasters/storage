import csv
import contextlib
import itertools
import sys

import tweepy

from common import *
from textsearch import gen_process

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage:", sys.argv[0], "<config file.py>", file = sys.stderr)
        exit(-1)

    api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    opts = {}
    with open(sys.argv[1], "r") as fd:
        exec(fd.read(), opts)

    with openconn() as conn, opencoll(conn, opts["COLLNAME"]) as coll:
        with contextlib.ExitStack() as exitstack:
            colls_in = [conn["twitter"][collname] for collname in opts["COLLNAME_STATUSES_A"]]
            sources = []

            for collname in opts["COLLNAME_STATUSES_C"]:
                cursor = conn["twitter"][collname].find(no_cursor_timeout = True)
                exitstack.enter_context(contextlib.closing(cursor))

                sources.append(cursor)

            for filename in opts["FILES_STATUSES_C"]:
                fd = exitstack.enter_context(open(filename, "r", newline = ""))

                if opts["CSV_DIALECT_OVERRIDE"] is None:
                    dialect = csv.Sniffer().sniff(fd.read(4096))
                    fd.seek(0)
                else:
                    dialect = opts["CSV_DIALECT_OVERRIDE"]

                sources.append(csv.DictReader(fd, dialect = dialect))

            process_f = gen_process(
                opts["GET_ID_FIELD"],
                opts["GET_TEXT_FIELD"],
                opts["GET_CATEGORIES_FIELD"],
                api,
                colls_in
            )

            # TODO: Thread this
            records = [process_f(r) for r in itertools.chain.from_iterable(sources)]

        coll.insert_many(list(filter(None, records)), ordered = False)
