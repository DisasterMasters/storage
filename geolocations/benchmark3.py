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
        "Statuses_ZtempOnePlace_A",
        "Statuses_ZtempTwoNoPlace_A",
        "Users_ZtempThree",
        "Geolocations_ZtempFour"
    ]

    with contextlib.ExitStack() as exitstack:
        db = exitstack.enter_context(opendb())

        for collname in tmpcolls_names:
            exitstack.callback(db.drop_collection, collname)

        statuses = [exitstack.enter_context(opencoll(db, collname)) for collname in statuses_names]
        users = [exitstack.enter_context(opencoll(db, collname)) for collname in users_names]
        tmpcolls = [exitstack.enter_context(opencoll(db, collname)) for collname in tmpcolls_names]

        fd_place = exitstack.enter_context(open("benchmark_place.csv", "a", newline = ""))
        fd_noplace = exitstack.enter_context(open("benchmark_noplace.csv", "a", newline = ""))
        csvfd_place = csv.writer(fd_place, quoting = csv.QUOTE_NONNUMERIC)
        csvfd_noplace = csv.writer(fd_noplace, quoting = csv.QUOTE_NONNUMERIC)

        os.rename("geolocations.db", "geolocations.db.bak")
        exitstack.callback(os.rename, "geolocations.db.bak", "geolocations.db")

        print("Constructing " + tmpcolls_names[1] + "...")

        for usercoll in users:
            tmpcolls[1].insert_many(usercoll.find())

        for n in [10000, 5000, 1000, 500, 100] * 3:
            assert n % len(statuses) == 0

            ct_place = 0
            ct_noplace = 0

            for statuscoll in statuses:
                for r in statuscoll.aggregate([{"$sample": {"size": n / len(statuses)}}], allowDiskUse = True):
                    if "place" in r or "coordinates" in r:
                        tmpcolls[0].insert_one(r)
                        ct_place += 1
                    else:
                        tmpcolls[1].insert_one(r)
                        ct_noplace += 1

            cmd = "python3 main.py " + tmpcolls_names[0] + " ".join(tmpcolls_names[2:]) + " > /dev/null"

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

            if ret == 0:
                print(str(datetime.timedelta(seconds = d1 - d0)) + " elapsed")
                csvfd_place.writerow([ct_place, d1 - d0])

            cmd = "python3 main.py " + " ".join(tmpcolls_names[1:]) + " > /dev/null"

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

            if ret == 0:
                print(str(datetime.timedelta(seconds = d1 - d0)) + " elapsed")
                csvfd_noplace.writerow([ct_noplace, d1 - d0])

            tmpcolls[0].delete_many({})
            tmpcolls[1].delete_many({})
            tmpcolls[3].delete_many({})
