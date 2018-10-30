import json
import socket
import sys
import pymongo
from pymongo import MongoClient

def usage():

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: %s [MongoDB collection]" % sys.argv [0], file = sys.stderr)
        exit(-1)

    conn = MongoClient("da1.eecs.utk.edu" if socket.gethostname() == "75f7e392a7ec" else "localhost")
    coll = conn['twitter'][sys.argv[1]]

    dups = set()
    ids = set()

    for r in coll.find({}, {'id': True}):
        if r['id'] in ids:
            dups.add(r['_id'])

        ids.add(r['id'])

    for id in dups:
        coll.remove({'_id': id})
