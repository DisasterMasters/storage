import copy
import datetime
from email.utils import parsedate_to_datetime
import itertools
from math import sin, cos, atan, sqrt, radians
import re
import threading
from urllib.request import urlopen
from urllib.parse import urlencode

import tweepy

from common import *

class OldKeywordThread(threading.Thread):
    max_id_regex = re.compile(r"max_id=(?P<max_id>\d+)")

    def __init__(self, queries, qu, ev):
        super().__init__()

        self.queries = " OR ".join(queries)
        self.qu = qu
        self.ev = ev

        self.api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    def run(self):
        max_id = None

        while not self.ev.wait(1):
            results = self.api.search(
                self.queries,
                result_type = "mixed",
                max_id = max_id,
                tweet_mode = "extended",
                include_entities = True,
                monitor_rate_limit = True,
                wait_on_rate_limit = True
            )

            timestamp = datetime.datetime.now(datetime.timezone.utc)

            if not results["statuses"]:
                break

            self.qu.put([adddates(statusconv(r), timestamp) for r in results["statuses"]])

            try:
                # This code _should_ work, but a lot of the time it crashes and
                # I don't know why. In that case, it's a somewhat safe
                # assumption that the tweet IDs are ordered, so just use the
                # smallest one we got so far as our new max_id
                max_id_match = OldKeywordThread.max_id_regex.search(results["search_metadata"]["next_results"])
                max_id = int(max_id_match.group("max_id"))
            except:
                max_id = min(results["statuses"], key = lambda r: r["id"])["id"] - 1

class OldUsernameThread(threading.Thread):
    def __init__(self, queries, qu, ev):
        super().__init__()

        self.queries = queries
        self.qu = qu
        self.ev = ev

        self.api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    def run(self):
        max_ids = {user: None for user in self.queries}
        skip = set()

        # Round-robin through each query
        for i in itertools.cycle(sorted(self.queries)):
            if self.ev.wait(1) or len(skip) == len(self.queries):
                break

            if i in skip:
                continue

            results = self.api.user_timeline(
                i,
                max_id = max_ids[i],
                tweet_mode = "extended",
                include_entities = True,
                monitor_rate_limit = True,
                wait_on_rate_limit = True
            )

            timestamp = datetime.datetime.now(datetime.timezone.utc)

            if not statuses:
                skip.add(i)
                continue

            self.qu.put([adddates(statusconv(r), timestamp) for r in results])

            max_id = statuses[-1]["id"] - 1

class OldLocationThread(threading.Thread):
    max_id_regex = re.compile(r"max_id=(?P<max_id>\d+)")

    def __init__(self, queries, qu, ev):
        super().__init__()

        self.queries = []

        for bbox in queries:
            xmin, ymin, xmax, ymax = bbox

            x = (xmin + xmax) / 2
            y = (ymin + ymax) / 2

            # Calculate the great circle distance between the two points, use
            # that as the diameter of the circle
            lat1 = radians(ymin)
            lat2 = radians(ymax)
            dlon = radians(abs(xmax - xmin))
            r = (6371.0088 / 2) * atan( \
                sqrt((cos(lat2) * sin(dlon)) ** 2 + (cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)) ** 2) / \
                (sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * sin(dlon)) \
            )

            self.queries.append("%f,%f,%fkm" % (y, x, r))

        self.qu = qu
        self.ev = ev

        self.api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser())

    def run(self):
        max_ids = {user: None for user in self.queries}
        skip = set()

        for i in itertools.cycle(sorted(self.queries)):
            if self.ev.wait(1) or len(skip) == len(self.queries):
                break

            if i in skip:
                continue

            results = self.api.search(
                geocode = i,
                result_type = "mixed",
                max_id = max_id,
                tweet_mode = "extended",
                include_entities = True,
                monitor_rate_limit = True,
                wait_on_rate_limit = True
            )

            timestamp = datetime.datetime.now(datetime.timezone.utc)

            if not results["statuses"]:
                skip.add(i)
                continue

            self.qu.put([adddates(statusconv(r), timestamp) for r in results["statuses"]])

            try:
                max_id_match = OldLocationThread.max_id_regex.search(results["search_metadata"]["next_results"])
                max_ids[i] = int(max_id_match.group("max_id"))
            except:
                max_ids[i] = min(results["statuses"], key = lambda r: r["id"])["id"] - 1
