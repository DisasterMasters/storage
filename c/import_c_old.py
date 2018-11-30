import contextlib
import csv
import os
import sys
import threading

import tweepy
import pymongo

def read_bsv(filename, coll, coll_mut):
    print(filename + ": Starting")

    if "media" in filename:
        categories = ["media"]
    elif "utility" in filename:
        categories = ["utility"]
    else:
        categories = []

    records = []

    with open(filename, "r", newline = '') as fd:
        reader = csv.DictReader(fd)

        for ln_csv in reader:
            tweet = {k: v for k, v in ln_csv.items() if k is not None}

            tweet["original_file"] = filename
            tweet["original_line"] = reader.line_num
            tweet["categories"] = categories

            records.append(tweet)

    if records:
        with coll_mut:
            dups = []

            for r in records:
                dups += [r["_id"] for r in coll.find({"id": r["id"]})]

            coll.delete_many({'_id': {'$in': dups}})
            coll.insert_many(records, ordered = False)

    print(filename + ": Done")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <DATA dir> <output collection>", file = sys.stderr)
        exit(-1)

    total_ct = 0
    success_ct = 0

    with contextlib.closing(pymongo.MongoClient()) as conn:
        coll = conn["twitter"][sys.argv[2]]
        coll_mut = threading.Lock()

        pool = []

        # Set up indices
        coll.create_index([('id', pymongo.HASHED)], name = 'id_index', sparse = True)
        coll.create_index([('id', pymongo.ASCENDING)], name = 'id_ordered_index', sparse = True)
        coll.create_index([('text', pymongo.TEXT)], name = 'search_index', default_language = 'english')

        for dirpath, _, filenames in os.walk(sys.argv[1]):
            if "media" in dirpath or "utility" in dirpath:
                for filename in filenames:
                    if filename[filename.rfind('.'):] == ".csv":
                        pool.append(threading.Thread(
                            target = read_bsv,
                            args = (os.path.join(dirpath, filename), coll, coll_mut)
                        ))

                        pool[-1].start()

        # Wait for all threads to finish
        for thrd in pool:
            thrd.join()
'''
def usage():
    print("Usage: %s [DATA dir] -o [MongoDB collection]" % sys.argv[0], file = sys.stderr)
    exit(-1)

def read_bsv(fd, coll):
    records = []

    id_regex = re.compile(r"((https?://)?(www\.)?twitter\.com)?/(\w{1,15}/status|statuses)/(?P<id>\d+)/?")
    fd_text = fd.read().splitlines()

    for ln, d in zip(fd_text[1:], csv.DictReader(fd_text, delimiter = "|", quoting = csv.QUOTE_NONE)):
        r = {k.strip("'").lower(): v for k, v in d.items() if k is not None}

        if "ID" in r:
            r["id"] = int(r["ID"])

            del r["ID"]
        else:
            id_match = id_regex.search(ln)

            if id_match is not None:
                r["id"] = int(id_match.group("id"))

        r["original_file"] = fd.name
        r["original_line"] = ln

        records.append(r)

    if records:
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
                    with open(os.path.join(dirpath, filename), "r") as fd:
                        print(fd.name)
                        read_bsv(fd, coll)

    # Delete duplicate tweets
    dups = []
    ids = set()

    for r in coll.find(projection = ["id"]):
        if "id" in r:
            if r['id'] in ids:
                dups.append(r['_id'])

            ids.add(r['id'])

    coll.delete_many({'_id': {'$in': dups}})
'''
