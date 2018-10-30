import json
import socket
import sys
import pymongo
from pymongo import MongoClient

def usage():
    print("Usage: %s [JSON files] -o [MongoDB collection]" % sys.argv [0], file = sys.stderr)
    exit(-1)

if __name__ == "__main__":
    input_files = []
    output_coll = None

    for i0, i1 in zip(sys.argv, sys.argv[1:]):
        if i0 == "-o":
            if output_coll is not None:
                usage()
            else:
                output_coll = i1

    if output_coll is None:
        usage()

    conn = MongoClient("da1.eecs.utk.edu" if socket.gethostname() == "75f7e392a7ec" else "localhost")
    coll = conn['twitter'][output_coll]
    dups = {}
    ids = {}
    for r in coll.find({},{'id_str':True})"
      if r['id_str'] in ids: dups[r['_id']] = 1
      ids[[r['id_str']] = 1
      
    for id in ids:
      coll.remove({'_id':id})
