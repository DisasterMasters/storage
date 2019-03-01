import csv
import contextlib
import math
import os
import sys
import time

from common import *

NITERS = 1000
NSTATUSES = lambda i: math.ceil((i + 100) * math.log(i + 2))
OUTFILE = "benchmark.csv"

def setup_tmpcolls(statuses, users, tmpcolls, n):
    for coll in tmpcolls:
        coll.delete_many({})

    for statuscoll, usercoll in zip(statuses, users):
        rs = list(statuscoll.aggregate([{"$sample": {"size": n / len(statuses)}}]))

        uids = set()
        for r in rs:
            uids |= {r["user"]["id"]}
            uids |= {user_mention["id"] for user_mention in r["entities"]["user_mentions"]}
            uids |= {user_mention["id"] for user_mention in r["extended_tweet"]["entities"]["user_mentions"]}

        us = list(usercoll.find({"id": {"$in": list(uids)}}))

        tmpcolls[0].insert_many(rs, ordered = False)
        tmpcolls[1].insert_many(us, ordered = False)


if __name__ == "__main__":
    statuses_names = [
        "Statuses_Irma_A",
        "Statuses_Maria_A",
        "Statuses_Florence_A"
    ]

    users_names = [
        "Users_Irma",
        "Users_Maria",
        "Users_Florence"
    ]

    tmpcolls_names = [
        "Statuses_ZtempOne_A",
        "Users_ZtempTwo",
        "Geolocations_ZtempThree"
    ]

    with contextlib.ExitStack() as exitstack:
        db = exitstack.enter_context(opendb())
        fd = exitstack.enter_context(open(OUTFILE, "a", newline = ""))
        csv_fd = csv.writer(fd, quoting = csv.QUOTE_NONNUMERIC)

        statuses = [exitstack.enter_context(opencoll(db, collname)) for collname in statuses_names]
        users = [exitstack.enter_context(opencoll(db, collname)) for collname in users_names]
        tmpcolls = [exitstack.enter_context(opencoll(db, collname)) for collname in tmpcolls_names]

        os.rename("geolocations.db", "geolocations.db.bak")
        exitstack.callback(os.rename, "geolocations.db.bak", "geolocations.db")

        for i in range(NITERS):
            setup_tmpcolls(statuses, users, tmpcolls, NSTATUSES(i))

            cmd = "python3 main.py " + " ".join(tmpcolls_names)

            d0 = time.perf_counter()
            ret = os.system(cmd)
            d1 = time.perf_counter()

            os.remove("geolocations.db")

            if ret == 0:
                csv_fd.writerow([NSTATUSES(i), d1 - d0])
