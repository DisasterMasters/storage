import json
import socket
import sys

from pymongo import MongoClient

def usage():
    print("Usage: %s [JSON files] -o [MongoDB collection]" % sys.argv[0], file = sys.stderr)
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
        elif i1 != "-o":
            input_files.append(i1)

    if len(input_files) == 0 or output_coll is None:
        usage()

    conn = MongoClient("da1.eecs.utk.edu" if socket.gethostname() == "75f7e392a7ec" else "localhost")
    coll = conn['twitter'][output_coll]

    for filename in input_files:
        with open(filename, "r") as fd:
            records = [json.loads(line.strip()) for line in fd if line.strip()]

        records = list(filter(lambda r: "id" in r or "id_str" in r, records))

        for r in records:
            if "id" in r:
                r["_id"] = r["id"]
            else:
                r["_id"] = int(r["id_str"])

        coll.insert_many(records, ordered = False)
