import csv
import os
import sys
import threading

from common import *

class ScrapyDialect(csv.Dialect):
    delimiter = "|"
    quotechar = "'"
    doublequote = True
    skipinitialspace = False
    lineterminator = "\n"
    quoting = csv.QUOTE_NONE

# Set this to avoid trusting sniffer
DIALECT_OVERRIDE = None

def read_csv(filename, coll, coll_mut):
    records = []

    with open(filename, "r", newline = '') as fd:
        if DIALECT_OVERRIDE is None:
            dialect = csv.Sniffer().sniff(fd.read(4096))
            fd.seek(0)
        else:
            dialect = DIALECT_OVERRIDE

        '''
        if filename[filename.rfind("."):] == ".txt":
            dialect = ScrapyDialect
            fieldnames = None
        else:
            blk = fd.read(4096)
            fd.seek(0)

            dialect = csv.Sniffer().sniff(blk)

            if csv.Sniffer().has_header(blk):
                fieldnames = None
            else:
                fieldnames = "username,date,retweets,favorites,text,geo,mentions,hashtags,id,permalink,FixedSpaceIssues".split(",")
        '''

        for row in csv.DictReader(fd, fieldnames, dialect = dialect):
            r = {k.strip("'"): v for k, v in row.items() if k}

            r["original_file"] = filename

            if "id" not in r and "ID" in r:
                r["id"] = r["ID"]
                del r["ID"]

            records.append(r)

    if records:
        with coll_mut:
            coll.insert_many(records, ordered = False)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: %s <CSV files/directories> <output collection>", file = sys.stderr)
        exit(-1)

    with openconn() as conn, opencoll(conn, sys.argv[-1], colltype = "statuses_c") as coll:
        coll_mut = threading.Lock()
        pool = []

        for arg in sys.argv[1:-1]:
            if os.path.isdir(arg):
                for dirpath, _, filenames in os.walk(arg):
                    for filename in filenames:
                        ext = filename[filename.rfind("."):]

                        if ext == ".csv" or ext == ".txt":
                            pool.append(threading.Thread(
                                target = read_csv,
                                args = (os.path.join(dirpath, filename), coll, coll_mut)
                            ))

                            pool[-1].start()
            else:
                pool.append(threading.Thread(
                    target = read_csv,
                    args = (arg, coll, coll_mut)
                ))

                pool[-1].start()

        # Wait for all threads to finish
        for thrd in pool:
            thrd.join()

        '''
        # Remove noisy entries
        rm = []
        ids = set()

        for r in coll.find(projection = ["id"]):
            try:
                id = int(r["id"])
            except:
                rm.append(r["_id"])
            else:
                if id in ids:
                    rm.append(r["_id"])

                ids.add(id)

        for i in range(0, len(rm), 800000):
            coll.delete_many({'_id': {"$in": rm[i:i + 800000]}})
        '''
