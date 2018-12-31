import copy
import sys

from common import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: %s <config file.py>" % sys.argv[0], file = sys.stderr)
        exit(-1)

    opts = {}
    with open(sys.argv[1], "r") as fd:
        exec(fd.read(), opts)

    rules = [(rule + (lambda x: x,) if len(rule) == 2 else rule) for rule in opts["RULES"]]
    assert all(len(rule) == 3 for rule in rules)

    post_f = opts["POST_FUNC"] if opts["POST_FUNC"] else lambda x: x

    with openconn() as conn, opencoll(conn, opts["COLLNAME"]) as coll:
        replacements = []

        for r in coll.find():
            new_r = {"original": copy.copy(r)}
            del new_r["original"]["_id"]

            for rule_from, rule_to, rule_f in rules:
                r_new[rule_to] = rule_f(r[rule_from])

            replacements.append((r["_id"], post_f(r_new)))

        for k, v in replacements:
            coll.replace_one({"_id": k}, v)
