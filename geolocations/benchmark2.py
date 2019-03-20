import csv
import contextlib
import datetime
import math
import os
import sys
import time

from common import *

if __name__ == "__main__":
    statuses_names = [
        "Statuses_Irma_A",
        "Statuses_Maria_A",
        "Statuses_Florence_A",
        "Statuses_MiscClimateChange_A",
        "Statuses_MiscPower_A"
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

        for collname in tmpcolls_names:
            exitstack.callback(db.drop_collection, collname)

        statuses = [exitstack.enter_context(opencoll(db, collname)) for collname in statuses_names]
        users = [exitstack.enter_context(opencoll(db, collname)) for collname in users_names]
        tmpcolls = [exitstack.enter_context(opencoll(db, collname)) for collname in tmpcolls_names]

        fd = exitstack.enter_context(open("benchmark.csv", "a", newline = ""))
        csv_fd = csv.writer(fd, quoting = csv.QUOTE_NONNUMERIC)

        os.rename("geolocations.db", "geolocations.db.bak")
        exitstack.callback(os.rename, "geolocations.db.bak", "geolocations.db")

        print("Constructing " + tmpcolls_names[1] + "...")

        for usercoll in users:
            tmpcolls[1].insert_many(usercoll.find())

        for n in [1000000, 100000, 10000, 1000, 100] * 3:
            assert n % len(statuses) == 0

            print("Constructing %s with %d random tweets..." % (tmpcolls_names[0], n))

            for statuscoll in statuses:
                tmpcolls[0].insert_many(statuscoll.aggregate([{"$sample": {"size": n / len(statuses)}}], allowDiskUse = True))

            cmd = "python3 main.py " + " ".join(tmpcolls_names) + " > /dev/null"

            print("Running \"" + cmd + "\"...")

            d0 = time.perf_counter()
            ret = os.system(cmd)
            d1 = time.perf_counter()

            try:
                os.remove("geolocations")
            except FileNotFoundError:
                pass

            try:
                os.remove("geolocations.db")
            except FileNotFoundError:
                pass

            try:
                os.remove("geolocations.db.db")
            except FileNotFoundError:
                pass

            tmpcolls[0].delete_many({})
            tmpcolls[2].delete_many({})

            if ret == 0:
                print(str(datetime.timedelta(seconds = d1 - d0)) + " elapsed")
                csv_fd.writerow([n, d1 - d0])
