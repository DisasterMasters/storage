import copy
from email.utils import parsedate_to_datetime
import re
import threading
from urllib.request import urlopen
from urllib.parse import urlencode
import datetime
import sys

import tweepy

sys.path.append("..")
from common import *

class OldKeywordThread(threading.Thread):
    max_id_regex = re.compile(r"max_id=(?P<max_id>\d+)")

    def __init__(self, queries, qu, ev):
        super().__init__()

        self.queries = queries
        self.qu = qu
        self.ev = ev

        self.api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    def run(self):
        max_id = None

        while not self.ev.wait(1):
            results = self.api.search(
                " OR ".join(self.queries),
                result_type = "mixed",
                max_id = max_id,
                tweet_mode = "extended",
                include_entities = True,
                monitor_rate_limit = True,
                wait_on_rate_limit = True
            )

            timestamp = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

            if not results["statuses"]:
                break

            self.qu.put([adddates(statusconv(r), timestamp) for r in results["statuses"]])

            max_id_match = OldKeywordThread.max_id_regex.search(results["search_metadata"]["next_results"])
            if max_id_match is not None:
                max_id = int(max_id_match.group("max_id"))
            else:
                max_id = min(statuses, key = lambda r: r["id"])["id"] - 1

class OldUsernameThread(threading.Thread):
    def __init__(self, queries, qu, ev):
        super().__init__()

        self.queries = queries
        self.qu = qu
        self.ev = ev

        self.api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    def run(self):
        for i in self.queries:
            max_id = None

            while not self.ev.wait(1):
                results = self.api.user_timeline(
                    i,
                    max_id = max_id,
                    tweet_mode = "extended",
                    include_entities = True,
                    monitor_rate_limit = True,
                    wait_on_rate_limit = True
                )

                timestamp = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

                if not statuses:
                    break

                self.qu.put([adddates(statusconv(r), timestamp) for r in results])

                max_id = statuses[-1]["id"] - 1
