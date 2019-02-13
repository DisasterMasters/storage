import copy
import collections
import sys

import nltk
import pymongo

from common import *
from geocode import GeolocationDB
from geojson import geojson_to_coords
from address import StreetAddress

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage:", sys.argv[0], "<input collection> <output collection>", file = sys.stderr)
        exit(-1)

    ctr = collections.Counter()

    with GeolocationDB("geolocations") as geodb:
        def get_coord_info(r):
            addr = None

            ctr["tweet"] += 1

            try:
                text = r["extended_tweet"]["full_text"]
            except KeyError:
                text = r["text"]

            if r["coordinates"] is not None:
                source = "coordinates"

                assert r["coordinates"]["type"] == "Point"

                lat = r["coordinates"]["coordinates"][1]
                lon = r["coordinates"]["coordinates"][0]
                geojson = r["coordinates"]

                ctr["tweet_geo"] += 1
            elif r["place"] is not None:
                source = "place"

                assert r["place"]["bounding_box"]["type"] == "Polygon"

                coords = r["place"]["bounding_box"]["coordinates"][0]

                if all(u == v for u, v in zip(coords, coords[1:] + coords[:1])):
                    lat = coords[0][1]
                    lon = coords[0][0]

                    geojson = {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    }
                else:
                    lat = sum(v[1] for v in coords) / len(coords)
                    lon = sum(v[0] for v in coords) / len(coords)

                    geojson = copy.deepcopy(r["place"]["bounding_box"])
                    if len(geojson["coordinates"][0]) == 4:
                        geojson["coordinates"][0].append(geojson["coordinates"][0][0])

                ctr["tweet_place"] += 1
            else:
                addr_nlp = StreetAddress.nlp(text)
                addr_re = StreetAddress.re(text)
                addr_statemap = StreetAddress.statemap(text)

                if addr_nlp is not None:
                    addr = addr_nlp
                    source = "address_nlp"

                    ctr["tweet_addr_nlp"] += 1
                elif addr_re is not None:
                    addr = addr_re
                    source = "address_re"

                    ctr["tweet_addr_re"] += 1
                elif addr_statemap is not None:
                    addr = addr_statemap
                    source = "address_statemap"

                    ctr["tweet_addr_statemap"] += 1
                else:
                    return None

                db_loc = geodb[addr]
                if db_loc is None:
                    return None

                ctr["tweet_geo_fromaddr"] += 1

                lat = float(db_loc["lat"])
                lon = float(db_loc["lon"])

                if "geojson" in db_loc:
                    geojson = db_loc["geojson"]
                elif "boundingbox" in db_loc:
                    geojson = {
                        "type": "Polygon",
                        "coordinates": [[
                            [float(db_loc["boundingbox"][0]), float(db_loc["boundingbox"][2])],
                            [float(db_loc["boundingbox"][0]), float(db_loc["boundingbox"][3])],
                            [float(db_loc["boundingbox"][1]), float(db_loc["boundingbox"][3])],
                            [float(db_loc["boundingbox"][1]), float(db_loc["boundingbox"][2])],
                            [float(db_loc["boundingbox"][0]), float(db_loc["boundingbox"][2])]
                        ]]
                    }
                else:
                    geojson = {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    }

            _, _, err = geojson_to_coords(geojson)

            if err is None:
                ctr["tweet_geo_errna"] += 1
            elif abs(err) < sys.float_info.epsilon:
                ctr["tweet_geo_0km"] += 1
            elif err <= 1.0:
                ctr["tweet_geo_0to1km"] += 1
            elif err <= 5.0:
                ctr["tweet_geo_1to5km"] += 1
            elif err <= 25.0:
                ctr["tweet_geo_5to25km"] += 1
            elif err <= 100.0:
                ctr["tweet_geo_25to100km"] += 1
            else:
                ctr["tweet_geo_gt100km"] += 1

            print("Tweet %r (\"%s\") mapped to (%f, %f) with %s" % (
                r["id"],
                text.replace("\n", "\\n"),
                lat, lon,
                ("an error of " + str(err) + " km") if err is not None else "an unknown error radius"
            ))

            return {
                "id": r["id"],
                "latitude": lat,
                "longitude": lon,
                "error": err,
                "source": source,
                "address": None if addr is None else addr._asdict(),
                "geojson": geojson
            }

        with opendb() as db, opencoll(db, sys.argv[1]) as coll_in, opencoll(db, sys.argv[2]) as coll_out:
            for r in coll_in.find():
                r = get_coord_info(r)

                if r is not None:
                    coll_out.insert_one(r)

    msg = """
Results for collecting geolocation info from %s to %s:
--------------------------------------------------------------------------------
Total tweets: %d
Tweets that have precise geolocation info in their metadata: %d
Tweets that have a place in their metadata: %d
--------------------------------------------------------------------------------
Tweets that have an address in their text: %d
* Tweets whose address was extracted via nlp(): %d
* Tweets whose address was extracted via re(): %d
* Tweets whose address was extracted via statemap(): %d
Tweets whose address mapped to a valid geolocation: %d
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
