import csv
import os
import sys
import threading

from common import *

'''
class ScrapyDialect(csv.Dialect):
    delimiter = "|"
    quotechar = "'"
    doublequote = True
    skipinitialspace = False
    lineterminator = "\n"
    quoting = csv.QUOTE_NONE
'''

def read_csv(opts, filename, coll, coll_mut):
    records = []

    print(filename + ": Starting")

    with open(filename, "r", newline = '') as fd:
        if opts["USE_SNIFFER"]:
            dialect = csv.Sniffer().sniff(fd.read(4096))
            fieldnames = None

            fd.seek(0)
        else:
            conf = opts["GET_DIALECT_FIELDNAMES_FUNC"](filename)

            if conf is None:
                print(filename + ": Done")
                return
            elif isinstance(conf, tuple):
                dialect, fieldnames = conf
            else:
                dialect = conf
                fieldnames = None

        dictreader = csv.DictReader(fd, fieldnames, dialect = dialect)

        for row in dictreader:
            r = opts["PREPROCESS_FUNC"](filename, row)

            if r is None:
                print("Bad line in %s:%d: {" % (filename, dictreader.line_num))
                for k, v in row.items():
                    print("\t%r: %r" % (k, v))
                print("}")
                continue

            # Strip superfluous single quotes
            r["original"] = {k if isinstance(k, str) else repr(k): v for k, v in row.items()}
            r["original_file"] = filename
            r["original_line"] = dictreader.line_num

            records.append(r)

    if records:
        with coll_mut:
            coll.insert_many(records, ordered = False)

    print(filename + ": Done")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: %s <config file>", file = sys.stderr)
        exit(-1)

    opts = {}
    with open(sys.argv[1], "r") as fd:
        exec(fd.read(), opts)

    with opendb() as db:
        coll = db[opts["COLLNAME"]]
        coll_mut = threading.Lock()
        pool = []

        for arg in opts["SRCS"]:
            if os.path.isdir(arg):
                for dirpath, _, filenames in os.walk(arg):
                    for filename in filenames:
                        pool.append(threading.Thread(
                            target = read_csv,
                            args = (opts, os.path.join(dirpath, filename), coll, coll_mut)
                        ))

                        pool[-1].start()
            else:
                pool.append(threading.Thread(
                    target = read_csv,
                    args = (opts, arg, coll, coll_mut)
                ))

                pool[-1].start()

        # Wait for all threads to finish
        for thrd in pool:
            thrd.join()
