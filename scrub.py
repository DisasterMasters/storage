import sys
import contextlib
from email.utils import parsedate_to_datetime
import socket

import pymongo

MONGODB_HOST = "da1.eecs.utk.edu" if socket.gethostname() == "75f7e392a7ec" else "localhost"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: %s <collection> [<collection> ...]", file = sys.stderr)
        exit(-1)

    with contextlib.closing(pymongo.MongoClient(MONGODB_HOST)) as conn:
        for collname in sys.argv[1:]:
            coll = conn["twitter"][collname]

            filter = {"$or": [
                {"retrieved_at": {"$type": "string"}},
                {"created_at": {"$type": "string"}},
                {"user.created_at": {"$type": "string"}},
                {"quoted_status.created_at": {"$type": "string"}}
            ]}
            projection = ["retrieved_at", "created_at", "user.created_at", "quoted_status.created_at"]

            for r in coll.find(filter, projection):
                repl = {}

                if "retrieved_at" in r and type(r["retrieved_at"]) is str:
                    repl["retrieved_at"] = parsedate_to_datetime(r["retrieved_at"])

                if "created_at" in r and type(r["created_at"]) is str:
                    repl["created_at"] = parsedate_to_datetime(r["created_at"])

                if "user" in r and type(r["user"]["created_at"]) is str:
                    repl["user.created_at"] = parsedate_to_datetime(r["user"]["created_at"])

                if "quoted_status" in r and type(r["quoted_status"]["created_at"]) is str:
                    repl["quoted_status.created_at"] = parsedate_to_datetime(r["quoted_status"]["created_at"])

                if repl:
                    coll.update_one({"_id": r["_id"]}, {"$set": repl})

            indices = [
                pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
                pymongo.IndexModel([('user.id', pymongo.HASHED)], name = 'user_id_index'),
                pymongo.IndexModel([('user.screen_name', pymongo.HASHED)], name = 'user_screen_name_index'),
                pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english'),
                pymongo.IndexModel([('created_at', pymongo.ASCENDING)], name = 'created_at_index'),
                pymongo.IndexModel([('categories', pymongo.ASCENDING)], name = 'categories_index', sparse = True)
            ]

            coll.drop_indexes()
            coll.create_indexes(indices)

            # Remove duplicates
            dups = []
            ids = set()

            for r in coll.find(projection = ["id"]):
                if r['id'] in ids:
                    dups.append(r['_id'])

                ids.add(r['id'])

            coll.delete_many({'_id': {'$in': dups}})
