import sys

import tweepy

from common import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <collection_from> <collection_to>", file = sys.stderr)
        exit(-1)

    api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    with openconn() as conn, opencoll(conn, sys.argv[1], colltype = "statuses_c") as coll_from, opencoll(conn, sys.argv[2]) as coll_to:
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

                retrieved_at = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

            except tweepy.TweepError:
                continue

            coll_to.insert_one(adddates(statusconv(r), retrieved_at))
