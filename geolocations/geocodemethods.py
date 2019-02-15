import copy
import statistics
import sys

import nltk
import pymongo

from common import *
from geocode import GeolocationDB
from geojson import geojson_to_coords
from address import StreetAddress

def __place(place, geodb):
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
    else if place["name"] is not None:
        tup = geodb[place["name"]]

        if tup is None:
            return None

        lat, lon, geojson = tup
    else:
        return None

    return (lat, lon, geojson)

def __streetaddress(text, geodb, addr_f):
    addr = addr_f(text)
    if addr is None:
        return None

    db_loc = geodb[addr]
    if db_loc is None:
        return None

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

    return (lat, lon, geojson)

def status_coordinates(status):
    if r["coordinates"] is None:
        return None

    assert r["coordinates"]["type"] == "Point"

    lat = r["coordinates"]["coordinates"][1]
    lon = r["coordinates"]["coordinates"][0]
    geojson = r["coordinates"]

    return (lat, lon, geojson)

def status_place(status, geodb):
    return __place(status["place"], geodb)

def status_streetaddress_nlp(status, geodb):
    try:
        text = r["extended_tweet"]["full_text"]
    except KeyError:
        text = r["text"]

    return __status_streetaddress(text, geodb, StreetAddress.nlp)

def status_streetaddress_re(status, geodb):
    try:
        text = r["extended_tweet"]["full_text"]
    except KeyError:
        text = r["text"]

    return __status_streetaddress(text, geodb, StreetAddress.re)

def status_streetaddress_statemap(status, geodb):
    try:
        text = r["extended_tweet"]["full_text"]
    except KeyError:
        text = r["text"]

    return __status_streetaddress(text, geodb, StreetAddress.statemap)

def user_place(user, geodb):
    if user is None:
        return None

    return __place(user["profile_location"], geodb)

def user_streetaddress_nlp(user, geodb):
    if user is None:
        return None

    return __status_streetaddress(user["description"], geodb, StreetAddress.nlp)

def user_streetaddress_re(user, geodb):
    if user is None:
        return None

    return __status_streetaddress(user["description"], geodb, StreetAddress.re)

def user_streetaddress_statemap(user, geodb):
    if user is None:
        return None

    return __status_streetaddress(user["description"], geodb, StreetAddress.statemap)

