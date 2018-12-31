import copy
import sys

from common import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: %s <config file.py>" % sys.argv[0], file = sys.stderr)
        exit(-1)

    # Woo, list comprehensions
    with open(sys.argv[1], "r") as fd:
        lns = [ln for ln in fd if ln.strip() and ln.strip()[0] != '#']

    rules = [tuple(s.strip() for s in ln.split("->")) for ln in lns[1:]]
    assert all(len(rule) == 2 for rule in rules)

    with openconn() as conn, opencoll(conn, lns[0].strip()) as coll:
        for r in coll.find():
            new_r = {"original": copy.copy(r)}
            del new_r["original"]["_id"]

            for rule_from, rule_to in rules:
                r_new[rule_to] = r[rule_from]

            coll.replace_one({"_id": r["_id"]}, r_new)
