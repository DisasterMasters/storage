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
    class ScrapyDialect(csv.Dialect):
        delimiter = "|"
        quotechar = "'"
        doublequote = True
        skipinitialspace = False
        lineterminator = "\n"
        quoting = csv.QUOTE_ALL

    records = [dict(row) for row in csv.DictReader(fd, dialect = ScrapyDialect)]

    for r in records:
        if "ID" in r:
            r["id"] = int(r["ID"])
            del r["ID"]
        elif "permalink" in r:
            r["id"] = int(r["permalink"].split("/")[-1])

        r["filename"] = fd.name

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
                if "_WCOORDS.txt" in filename:
                    with open(os.path.join(dirpath, filename), "r") as fd:
                        read_bsv(fd, coll)

    # Delete duplicate tweets
    dups = []
    ids = set()

    for r in coll.find(projection = ["id"]):
        if r['id'] in ids:
            dups.append(r['_id'])

        ids.add(r['id'])

    coll.delete_many({'_id': {'$in': dups}})
