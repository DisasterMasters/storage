import json
import socket
import sys

from pymongo import MongoClient

def usage():
    print("Usage: %s [JSON files] -o [MongoDB collection]" % sys.argv[0], file = sys.stderr)
    exit(-1)

if __name__ == "__main__":
    args = list(zip(sys.argv, sys.argv[1:]))

    input_files = [i1 for i0, i1 in args if i0 != "-o" and i1 != "-o"]
    output_coll = None

    for i0, i1 in args:
        if i0 == "-o":
            if output_coll is not None:
                usage()
            else:
                output_coll = i1

    if len(input_files) == 0 or output_coll is None:
        usage()

    conn = MongoClient("da1.eecs.utk.edu" if socket.gethostname() == "75f7e392a7ec" else "localhost")
    coll = conn['twitter'][output_coll[0]]

    for filename in input_files:
        with open(filename, "r") as fd:
            records = [json.loads(line) for line in fd]

        for r in records:
            r["_id"] = r["id"]

        coll.insert_many(records, ordered = False)
