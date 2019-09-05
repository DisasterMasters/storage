from email.utils import format_datetime
import sys

import tweepy

from common import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: " + sys.argv[0] + " <collection_from> <collection_to>", file = sys.stderr)
        exit(-1)

    api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    with opendb() as db:
        coll_from = db[sys.argv[1]]
        coll_to = db[sys.argv[2]]

        for r0 in coll_from.find(projection = ["id"]):
            if coll_to.find({"id": r0["id"]}, projection = ["id"]) is not None:
                continue

            try:
                r = api.get_status(
                    id,
                    tweet_mode = "extended",
                    include_entities = True,
                    monitor_rate_limit = True,
                    wait_on_rate_limit = True
                )

                retrieved_at = datetime.datetime.now(datetime.timezone.utc)

            except tweepy.TweepError:
                continue

            print("\"%s\" -- @%s, %s (retrieved %s)" % (
                r["text"],
                r["user"]["screen_name"],
                r["created_at"],
                format_datetime(retrieved_at)
            ))

            coll_to.insert_one(adddates(statusconv(r), retrieved_at))
