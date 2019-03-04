import csv
import contextlib
import math
import os
import sys
import time

from common import *

NSTOP = 10000
NREM = math.ceil(math.sqrt(x))
OUTFILE = "benchmark.csv"

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
        "Statuses_ZtempFour_A",
        "Users_ZtempFive",
        "Geolocations_ZtempSix"
    ]

    with contextlib.ExitStack() as exitstack:
        db = exitstack.enter_context(opendb())
        fd = exitstack.enter_context(open(OUTFILE, "a", newline = ""))
        csv_fd = csv.writer(fd, quoting = csv.QUOTE_NONNUMERIC)

        tmpcolls = [exitstack.enter_context(opencoll(db, collname)) for collname in tmpcolls_names]

        os.rename("geolocations.db", "geolocations.db.bak")
        exitstack.callback(os.rename, "geolocations.db.bak", "geolocations.db")

        for collname in statuses_names:
            tmpcolls[0].insert_many(db[collname].find())

        for collname in users_names:
            tmpcolls[1].insert_many(db[collname].find())

        ct = db.command("collstats", tmpcolls_names[0])["count"]

        while ct > NSTOP:
            cmd = "python3 main.py " + " ".join(tmpcolls_names)

            d0 = time.perf_counter()
            ret = os.system(cmd)
            d1 = time.perf_counter()

            os.remove("geolocations.db")
            tmpcolls[2].delete_many({})

            if ret == 0:
                csv_fd.writerow([ct, d1 - d0])

            ids = list(tmpcolls[0].aggregate([
                {"$sample": {"size": NREM(ct)}},
                {"$project": {"_id": True}}
            ]))

            tmpcolls[0].delete_many({"_id": {"$in": ids}})

            ct -= NREM(ct)
