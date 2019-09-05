import copy
import statistics
import sys

from common import *
from streetaddress import StreetAddress

__all__ = [
    "status_coordinates",
    "status_place",
    "status_streetaddress_nlp",
    "status_streetaddress_re",
    "status_streetaddress_statemap",
    "user_place",
    "user_streetaddress_nlp",
    "user_streetaddress_re",
    "user_streetaddress_statemap"
]

def __geolocationdb_process(db_loc):
    lat = float(db_loc["lat"])
    lon = float(db_loc["lon"])

    if "geojson" in db_loc:
        geojson = db_loc["geojson"]
    elif "boundingbox" in db_loc:
        geojson = {
            "type": "Polygon",
            "coordinates": [[
                [float(db_loc["boundingbox"][2]), float(db_loc["boundingbox"][0])],
                [float(db_loc["boundingbox"][3]), float(db_loc["boundingbox"][0])],
                [float(db_loc["boundingbox"][3]), float(db_loc["boundingbox"][1])],
                [float(db_loc["boundingbox"][2]), float(db_loc["boundingbox"][1])],
                [float(db_loc["boundingbox"][2]), float(db_loc["boundingbox"][0])]
            ]]
        }
    else:
        geojson = {
            "type": "Point",
            "coordinates": [lon, lat]
        }

    return lat, lon, geojson

def __place(place, geodb, source):
    if place is None:
        return None

    if place["bounding_box"] is not None:
        assert place["bounding_box"]["type"] == "Polygon"

        coords = place["bounding_box"]["coordinates"][0]

        if all(u == v for u, v in zip(coords, coords[1:] + coords[:1])):
            lat = coords[0][1]
            lon = coords[0][0]

            geojson = {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        else:
            lat = statistics.mean(v[1] for v in coords)
            lon = statistics.mean(v[0] for v in coords)

            geojson = copy.deepcopy(place["bounding_box"])

            if len(geojson["coordinates"][0]) == 4:
                geojson["coordinates"][0].append(geojson["coordinates"][0][0])
    elif place["name"] is not None:
        db_loc = geodb[place["name"]]

        if db_loc is None:
            return None

        lat, lon, geojson = __geolocationdb_process(db_loc)
    else:
        return None

    return {
        "latitude": lat,
        "longitude": lon,
        "geojson": geojson,
        "source": source,
        "address": None
    }

def __streetaddress(text, geodb, source, addr_f):
    addr = addr_f(text)
    if addr is None:
        return None

    db_loc = geodb[addr]
    if db_loc is None:
        return None

    lat, lon, geojson = __geolocationdb_process(db_loc)

    return {
        "latitude": lat,
        "longitude": lon,
        "geojson": geojson,
        "source": source,
        "address": addr._asdict()
    }

def status_coordinates(status, user, geodb):
    if status["coordinates"] is None:
        return None

    assert status["coordinates"]["type"] == "Point"

    lat = status["coordinates"]["coordinates"][1]
    lon = status["coordinates"]["coordinates"][0]
    geojson = status["coordinates"]

    return {
        "latitude": lat,
        "longitude": lon,
        "geojson": geojson,
        "source": "status_coordinates",
        "address": None
    }

def status_place(status, user, geodb):
    return __place(status["place"], geodb, "status_place")

def status_streetaddress_nlp(status, user, geodb):
    return __streetaddress(getnicetext(status), geodb, "status_streetaddress_nlp", StreetAddress.nlp)

def status_streetaddress_re(status, user, geodb):
    return __streetaddress(getnicetext(status), geodb, "status_streetaddress_re", StreetAddress.re)

def status_streetaddress_statemap(status, user, geodb):
    return __streetaddress(getnicetext(status), geodb, "status_streetaddress_statemap", StreetAddress.statemap)

def user_place(status, user, geodb):
    if user is None:
        return None

    return __place(user["profile_location"], geodb, "user_place")

def user_streetaddress_nlp(status, user, geodb):
    if user is None:
        return None

    return __streetaddress(user["description"], geodb, "user_streetaddress_nlp", StreetAddress.nlp)

def user_streetaddress_re(status, user, geodb):
    if user is None:
        return None

    return __streetaddress(user["description"], geodb, "user_streetaddress_re", StreetAddress.re)

def user_streetaddress_statemap(status, user, geodb):
    if user is None:
        return None

    return __streetaddress(user["description"], geodb, "user_streetaddress_statemap", StreetAddress.statemap)

