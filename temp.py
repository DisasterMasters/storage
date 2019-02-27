from common import *
import sys

with opendb() as db:
    def get_count(coll, user):
        collct = db.command("collstats", coll.name)["count"]
        userct = db.command("collstats", user.name)["count"]
        percent = 100.0 * (collct / userct)

        print("%s: %d entries (%.02f of %s)" % (coll.name, collct, percent, user.name))

    colla = db["Geolocations_Irma"]
    collb = db["Geolocations_Maria"]
    collc = db["Geolocations_Florence"]

    usera = db["Statuses_Irma_A"]
    userb = db["Statuses_Maria_A"]
    userc = db["Statuses_Florence_A"]

    get_count(colla, usera)
    get_count(collb, userb)
    get_count(collc, userc)
    print("")

    methods = [
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

    alldocs = list(colla.find(projection = ["source"])) + \
              list(collb.find(projection = ["source"])) + \
              list(collc.find(projection = ["source"]))
    totalct = len(alldocs)

    for method in methods:
        collct = len([True for x in alldocs if x["source"] == method])
        percent = 100.0 * (collct / totalct)

        print("%s: %d entries (%.02f of total)" % (method, collct, percent))
