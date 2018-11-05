import csv
import json
import os
import socket
import sys

import pymongo

def usage():
    print("Usage: %s [DATA dir] -o [MongoDB collection]" % sys.argv[0], file = sys.stderr)
    exit(-1)

def read_bsv(fd, coll):
    records = []

    for row in csv.DictReader(fd, delimiter = "|", quoting = csv.QUOTE_NONE):
        r = {}

        if "'ID'" in row:
            r["id"] = int(row["'ID'"])
        elif "'permalink'" in row:
            r["id"] = int(row["'permalink'"].split("/")[-1])

        r["filename"] = fd.name

        for k, v in row.items():
            if k is not None and k != "'ID'":
                r[k.strip("'").lower()] = v

        records.append(r)

    coll.insert_many(records, ordered = False)

if __name__ == "__main__":
    input_dirs = []
    output_coll = None

    for i0, i1 in zip(sys.argv, sys.argv[1:]):
        if i0 == "-o":
            if output_coll is not None:
                usage()
            else:
                output_coll = i1
        elif i1 != "-o":
            input_dirs.append(i1)

    if len(input_dirs) == 0 or output_coll is None:
        usage()

    conn = pymongo.MongoClient("da1.eecs.utk.edu" if socket.gethostname() == "75f7e392a7ec" else "localhost")
    coll = conn['twitter'][output_coll]

    # Make tweets indexable by id and text fields
    coll.create_index([('id', pymongo.HASHED)], name = 'id_index')
    coll.create_index([('id', pymongo.ASCENDING)], name = 'id_ordered_index')
    coll.create_index([('text', pymongo.TEXT)], name = 'search_index', default_language = 'english')

    for dir in input_dirs:
        for dirpath, _, filenames in os.walk(dir):
            for filename in filenames:
                if filename[filename.rfind('.'):] == ".txt" and not "_WCOORDS" in filename:
                    with open(os.path.join(dirpath, filename), "r", newline = '') as fd:
                        read_bsv(fd, coll)

    # Delete duplicate tweets
    dups = []
    ids = set()

    for r in coll.find(projection = ["id"]):
        if r['id'] in ids:
            dups.append(r['_id'])

        ids.add(r['id'])

    coll.delete_many({'_id': {'$in': dups}})
