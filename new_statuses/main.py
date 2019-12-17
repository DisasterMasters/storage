import collections
import configparser
import datetime
from email.utils import format_datetime
import itertools
import json
import math
import operator
import queue
import sys
import threading

from bson import BSON
from fuzzywuzzy import fuzz
from shapely.geometry import shape
import tweepy

from common import *

class QueueListener(tweepy.StreamListener):
    def __init__(self, qu, ev):
        super().__init__()

        self.qu = qu
        self.ev = ev

    def on_data(self, data):
        retrieved_at = datetime.datetime.now(datetime.timezone.utc)
        status = json.loads(data)

        if "in_reply_to_status_id" in status:
            self.qu.put(adddates(status, retrieved_at))

        return not self.ev.wait(0)

    def on_error(self, status_code):
        if status_code == 420:
            return not self.ev.wait(60)
        elif status_code // 100 == 5:
            return not self.ev.wait(5)

def getnewtweets(qu, ev, keywords = [], usernames = [], locations = []):
    strm = tweepy.Stream(auth = TWITTER_CREDENTIALS, listener = QueueListener(qu, ev))
    kwargs = {}

    if keywords:
        kwargs["track"] = keywords[:]

    if usernames:
        api = tweepy.API(TWITTER_CREDENTIALS)
        kwargs["follow"] = [api.get_user(username).id_str for username in usernames]

    if locations:
        kwargs["locations"] = [i for location in locations for i in shape(location).bounds]

    while True:
        try:
            strm.filter(**kwargs)
        except:
            if ev.wait(5):
                break
        else:
            break

def getoldtweets(qu, ev, keywords = [], usernames = [], locations = []):
    api = tweepy.API(TWITTER_CREDENTIALS, parser = tweepy.parsers.JSONParser())
    queries = []

    if keywords:
        # Search queries can be at most 500 characters, so split into multiple
        # queries if one isn't enough

        def generate_keyword_queries(k):
            if len(k) == 0:
                return []

            query = " OR ".join(k) + " -filter:retweets"

            if len(query) <= 500:
                return [query]
            else:
                return itertools.chain(
                    generate_keyword_list(k[:len(k) // 2]),
                    generate_keyword_list(k[len(k) // 2:])
                )

        for query in generate_keyword_queries(keywords):
            queries.append(("keyword", query, None))

    if usernames:
        for username in usernames:
            queries.append(("username", username, None))

    if locations:
        for location in locations:
            xmin, ymin, xmax, ymax = shape(location).bounds
            x = (xmin + xmax) / 2
            y = (ymin + ymax) / 2

            # Calculate the great circle distance between the two points, use
            # that as the diameter of the circle
            lat1 = math.radians(ymin)
            lat2 = math.radians(ymax)
            dlon = math.radians(abs(xmax - xmin))
            r = (6371.0088 / 2) * math.atan2(
                math.sqrt((math.cos(lat2) * math.sin(dlon)) ** 2 + (math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)) ** 2),
                math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(dlon)
            )

            queries.append(("location", "%f,%f,%fkm" % (y, x, r), None))

    while queries:
        next_queries = []

        for query_type, query, max_id in queries:
            if ev.wait(1):
                return

            if max_id is None:
                if query_type == "keyword":
                    statuses = api.search(
                        query,
                        result_type = "mixed",
                        tweet_mode = "extended",
                        include_entities = True,
                        monitor_rate_limit = True,
                        wait_on_rate_limit = True
                    ).get("statuses", [])

                elif query_type == "username":
                    statuses = api.user_timeline(
                        query,
                        tweet_mode = "extended",
                        include_entities = True,
                        monitor_rate_limit = True,
                        wait_on_rate_limit = True
                    )

                elif query_type == "location":
                    statuses = api.search(
                        geocode = query,
                        result_type = "mixed",
                        tweet_mode = "extended",
                        include_entities = True,
                        monitor_rate_limit = True,
                        wait_on_rate_limit = True
                    ).get("statuses", [])

            else:
                if query_type == "keyword":
                    statuses = api.search(
                        query,
                        result_type = "mixed",
                        max_id = max_id,
                        tweet_mode = "extended",
                        include_entities = True,
                        monitor_rate_limit = True,
                        wait_on_rate_limit = True
                    ).get("statuses", [])

                elif query_type == "username":
                    statuses = api.user_timeline(
                        query,
                        max_id = max_id,
                        tweet_mode = "extended",
                        include_entities = True,
                        monitor_rate_limit = True,
                        wait_on_rate_limit = True
                    )

                elif query_type == "location":
                    statuses = api.search(
                        geocode = query,
                        result_type = "mixed",
                        max_id = max_id,
                        tweet_mode = "extended",
                        include_entities = True,
                        monitor_rate_limit = True,
                        wait_on_rate_limit = True
                    ).get("statuses", [])

            timestamp = datetime.datetime.now(datetime.timezone.utc)

            if statuses:
                for status in statuses:
                    qu.put(adddates(statusconv(status), timestamp))

                next_max_id = min(statuses, key = operator.itemgetter("id"))["id"] - 1
                next_queries.append((query_type, query, next_max_id))

        queries = next_queries

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: " + sys.argv[0] + "<config_file.ini>", file = sys.stderr)
        exit(-1)

    parser = configparser.ConfigParser()
    parser.read(sys.argv[1])

    #parser["DEFAULT"] = {
    #    "keywords": "[]",
    #    "usernames": "[]",
    #    "locations": "[]",
    #    "old_keywords": "[]",
    #    "old_usernames": "[]",
    #    "old_locations": "[]",
    #    "new_keywords": "[]",
    #    "new_usernames": "[]",
    #    "new_locations": "[]"
    #}

    old_keywords = set()
    old_usernames = set()
    old_locations = []

    new_keywords = set()
    new_usernames = set()
    new_locations = []

    #SPATIAL_HASH_GRANULARITY = 5

    keyword_map = collections.defaultdict(set)
    username_map = collections.defaultdict(set)
    #location_map = [[[] for _ in range(180 // SPATIAL_HASH_GRANULARITY)] for _ in range(360 // SPATIAL_HASH_GRANULARITY)]
    location_map = []

    try:
        qu = queue.SimpleQueue()
    except AttributeError: # Fix for Python <3.7
        qu = queue.Queue()

    ev = threading.Event()

    for section in parser.sections():
        collname = "statuses_a:" + section
        collparams = collections.defaultdict(list, ((k, json.loads(v)) for k, v in parser[section].items()))

        for key in ["locations", "old_locations", "new_locations"]:
            if key not in collparams:
                collparams[key] = None

        old_keywords.update(collparams["keywords"])
        old_keywords.update(collparams["old_keywords"])

        old_usernames.update(collparams["usernames"])
        old_usernames.update(collparams["old_usernames"])

        old_locations.append(collparams["locations"])
        old_locations.append(collparams["old_locations"])

        new_keywords.update(collparams["keywords"])
        new_keywords.update(collparams["new_keywords"])

        new_usernames.update(collparams["usernames"])
        new_usernames.update(collparams["new_usernames"])

        new_locations.append(collparams["locations"])
        new_locations.append(collparams["new_locations"])

        for keyword in itertools.chain(collparams["keywords"], collparams["old_keywords"], collparams["new_keywords"]):
            keyword_map[keyword].add(collname)

        for username in itertools.chain(collparams["usernames"], collparams["old_usernames"], collparams["new_usernames"]):
            username_map[username].add(collname)

        for location in [collparams["locations"], collparams["old_locations"], collparams["new_locations"]]:
            if location is not None:
                location_map.append((shape(location), collname))

    old_keywords = list(old_keywords)
    old_usernames = list(old_usernames)

    new_keywords = list(new_keywords)
    new_usernames = list(new_usernames)

    old_locations = [location for location in old_locations if location is not None]
    new_locations = [location for location in new_locations if location is not None]

    old_thrd = threading.Thread(target = getoldtweets, args = (qu, ev, old_keywords, old_usernames, old_locations))
    new_thrd = threading.Thread(target = getnewtweets, args = (qu, ev, new_keywords, new_usernames, new_locations))

    old_thrd.start()
    new_thrd.start()

    #stop_sentinel = object()

    with opendb() as db, open("bitbucket.bson", "ab") as bitbucket:
        try:
            while True:
                status = qu.get()

                colls_to_insert = set()
                text = getnicetext(status)

                for keyword, collnames in keyword_map.items():
                    if fuzz.partial_ratio(keyword, text) > 80:
                        colls_to_insert |= collnames

                if status["user"]["screen_name"] in username_map:
                    colls_to_insert |= username_map[status["user"]["screen_name"]]

                if status["coordinates"] is not None:
                    status_location = shape(status["coordinates"])
                elif status["place"] is not None and status["place"]["bounding_box"] is not None:
                    status_location = shape(status["place"]["bounding_box"])
                else:
                    status_location = None

                if status_location is not None:
                    for location, collname in location_map:
                        if location.intersection(status_location).area > 0.0:
                            colls_to_insert.add(collname)

                if colls_to_insert:
                    for collname in colls_to_insert:
                        print("\"%s\" -- @%s, created %s, retrieved %s => %s" % (
                            getnicetext(status),
                            status["user"]["screen_name"],
                            format_datetime(status["created_at"]),
                            format_datetime(status["retrieved_at"]),
                            collname
                        ))

                        db[collname].insert_one(status)
                else:
                    bitbucket.write(BSON.encode(status))

        except KeyboardInterrupt:
            ev.set()

    old_thrd.join()
    new_thrd.join()
