import contextlib
import copy
import collections
import sys

import nltk
import pymongo

from common import *
from geolocationdb import GeolocationDB
from geocodemethods import *
from error import geojson_error

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage:", sys.argv[0], "<statuses collection> <users collection> <output collection>", file = sys.stderr)
        exit(-1)

    ctr = collections.Counter()

    with GeolocationDB("geolocations.db") as geodb:
        def get_coord_info(status, user):
            methods = [
                status_coordinates,
                status_place,
                status_streetaddress_nlp,
                status_streetaddress_re,
                status_streetaddress_statemap,
                user_place,
                user_streetaddress_nlp,
                user_streetaddress_re,
                user_streetaddress_statemap
            ]

            for method_f in methods:
                r = method_f(status, user, geodb)

                if r is not None:
                    ctr["tweet_" + r["source"]] += 1
                    break
            else:
                return None

            r["id"] = status["id"]
            r["error"] = geojson_error(r["latitude"], r["longitude"], r["geojson"])
            r["retrieved_at"] = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

            if r["error"] is None:
                ctr["tweet_geo_errna"] += 1
            elif abs(r["error"]) < sys.float_info.epsilon:
                ctr["tweet_geo_0km"] += 1
            elif r["error"] <= 1.0:
                ctr["tweet_geo_0to1km"] += 1
            elif r["error"] <= 5.0:
                ctr["tweet_geo_1to5km"] += 1
            elif r["error"] <= 25.0:
                ctr["tweet_geo_5to25km"] += 1
            elif r["error"] <= 100.0:
                ctr["tweet_geo_25to100km"] += 1
            else:
                ctr["tweet_geo_gt100km"] += 1

            print("Tweet %r (\"%s\") mapped to (%f, %f) via %s with %s" % (
                r["id"],
                status["text"].replace("\n", "\\n"),
                r["latitude"], r["longitude"],
                r["source"],
                ("an error of " + str(r["error"]) + " km") if r["error"] is not None else "an unknown error radius"
            ))

            return r

        with contextlib.ExitStack() as exitstack:
            db = exitstack.enter_context(opendb())
            coll_statuses = exitstack.enter_context(opencoll(db, sys.argv[1]))
            coll_users = exitstack.enter_context(opencoll(db, sys.argv[2]))
            coll_out = exitstack.enter_context(opencoll(db, sys.argv[3]))

            for status in coll_statuses.find():
                user = coll_users.find_one({"id": status["user"]["id"]})

                r = get_coord_info(status, user)

                if r is not None:
                    coll_out.insert_one(r)

    msg = """
Results for collecting geolocation info from %s to %s:
--------------------------------------------------------------------------------
Total tweets: %d
--------------------------------------------------------------------------------
Tweets that have an address in their text: %d
* Tweets whose address was extracted via nlp(): %d
* Tweets whose address was extracted via re(): %d
* Tweets whose address was extracted via statemap(): %d
Tweets whose users have an address in their text: %d
* Users whose address was extracted via nlp(): %d
* Users whose address was extracted via re(): %d
* Users whose address was extracted via statemap(): %d
Tweets with a non-empty "coordinates" field: %d
Tweets with a non-empty "place" field: %d
Tweets whose users have a non-empty "place" field: %d
--------------------------------------------------------------------------------
Tweets whose geolocation error is equal to 0 km: %d
Tweets whose geolocation error is in the range (0 km, 1 km]: %d
Tweets whose geolocation error is in the range (1 km, 5 km]: %d
Tweets whose geolocation error is in the range (5 km, 25 km]: %d
Tweets whose geolocation error is in the range (25 km, 100 km]: %d
Tweets whose geolocation error is greater than 100 km: %d
Tweets whose geolocation error couldn't be calculated: %d""" % (
        sys.argv[1], sys.argv[2],
        ctr["tweet"],
        ctr["tweet_geo"],
        ctr["tweet_place"],
        ctr["tweet_addr"],
        ctr["tweet_addr_nlp"],
        ctr["tweet_addr_re"],
        ctr["tweet_addr_statemap"],
        ctr["tweet_geo_fromaddr"],
        ctr["tweet_geo_0km"],
        ctr["tweet_geo_0to1km"],
        ctr["tweet_geo_1to5km"],
        ctr["tweet_geo_5to25km"],
        ctr["tweet_geo_25to100km"],
        ctr["tweet_geo_gt100km"],
        ctr["tweet_geo_errna"]
    )

    print(msg)
