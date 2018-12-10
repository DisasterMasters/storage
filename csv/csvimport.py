import csv
import os
import sys
import threading

from common import *

def read_csv(filename, coll, coll_mut):
    ext = filename[filename.rfind("."):]

    if ext != ".csv" && ext != ".txt":
        return

    '''
    class ScrapyDialect(csv.Dialect):
        delimiter = "|"
        quotechar = "'"
        doublequote = True
        skipinitialspace = False
        lineterminator = "\n"
        quoting = csv.QUOTE_MINIMAL
    '''

    records = []

    with open(filename, "r", newline = '') as fd:
        dialect = csv.Sniffer().sniff(fd.read(4096))
        fd.seek(0)

        for row in csv.DictReader(fd, dialect = dialect):
            r = {k: v for k, v in row.items() if k}

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
