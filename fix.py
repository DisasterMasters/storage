import contextlib
from email.utils import parsedate_to_datetime
import sys

from common import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        exit(-1)

    ct = 0

    with opendb() as db, opencoll(db, sys.argv[1]) as coll:
        cursor = coll.find(
            filter = {"$or": [{"quoted_status": {"$exists": True}}, {"retweeted_status": {"$exists": True}}]},
            projection = ["quoted_status.user.created_at", "retweeted_status"],
            no_cursor_timeout = True
        )

        with contextlib.closing(cursor) as c:
            for r in c:
                update_doc = {}

                if "quoted_status" in r:
                    update_doc["quoted_status.user.created_at"] = parsedate_to_datetime(r["quoted_status"]["user"]["created_at"])

                if "retweeted_status" in r:
                    update_doc["retweeted_status"] = adddates(statusconv(r["retweeted_status"]))

                coll.update_one({"_id": r["_id"]}, {"$set": update_doc})
                ct += 1

        print("Updated %d entries" % ct)
