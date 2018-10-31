import json
import socket
import sys

import pymongo

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

    conn = pymongo.MongoClient("da1.eecs.utk.edu" if socket.gethostname() == "75f7e392a7ec" else "localhost")
    coll = conn['twitter'][output_coll]

    # Make tweets indexable by id and text fields
    coll.create_index([('id', pymongo.ASCENDING)], name = 'id_index')
    coll.create_index([('text', pymongo.TEXT)], name = 'search_index', default_language = 'english')

    for filename in input_files:
        map_f    = lambda ln: json.loads(ln.strip()) if ln.strip() else {}
        filter_f = lambda r:  type(r) is dict and "id" in r

        with open(filename, "r") as fd:
            records = list(filter(filter_f, map(map_f, fd)))

        coll.insert_many(records, ordered = False)

    # Delete duplicate tweets
    dups = set()
    ids = set()

    for r in coll.find({}, {'id': True}):
        if r['id'] in ids:
            dups.add(r['_id'])

        ids.add(r['id'])

    for id in dups:
        coll.remove({'_id': id})
