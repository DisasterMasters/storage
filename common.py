import contextlib
import copy
import datetime
from email.utils import parsedate_to_datetime
import os
import re
import socket
import time
from urllib.error import HTTPError
from urllib.request import urlopen
from urllib.parse import urlencode

import pymongo
import tweepy

try:
    NullContext = lambda *args, **kwargs: contextlib.nullcontext()
except AttributeError: # Fix for Python <3.7
    class NullContext:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            pass

        def __exit__(self):
            pass

try:
    from sshtunnel import SSHTunnelForwarder
except ImportError:
    SSHTunnelForwarder = NullContext

# Twitter API authentication token for this project
TWITTER_AUTHKEY = tweepy.OAuthHandler(
    "ZFVyefAyg58PTdG7m8Mpe7cze",
    "KyWRZ9QkiC2MiscQ7aGpl5K2lbcR3pHYFTs7SCVIyxMlVfGjw0"
)
TWITTER_AUTHKEY.set_access_token(
    "1041697847538638848-8J81uZBO1tMPvGHYXeVSngKuUz7Cyh",
    "jGNOVDxllHhO57EaN2FVejiR7crpENStbZ7bHqwv2tYDU"
)

# To maintain backwards-compatibility
TWITTER_AUTH = TWITTER_AUTHKEY

@contextlib.contextmanager
def opendb(*, hostname = None, dbname = "twitter"):
    """
    Opens the MongoDB database, creating a connection to the Docker container
    if necessary. If hostname isn't specified, then it checks the MONGODB_HOST
    environment variable, and if that is unset, it checks to see if we're
    already running on the Docker. If all else fails, it opens up

    :param hostname str: URI of the MongoDB database
    :param dbname str: Name of the database to open
    :return: A context manager that yields the database
    """

    ssh_tunnel = NullContext()

    if hostname is None:
        if os.environ.get("MONGODB_HOST") is not None:
            hostname = os.environ.get("MONGODB_HOST")
        elif socket.gethostname() == "75f7e392a7ec":
            hostname = "da1.eecs.utk.edu"
        else:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                open_tunnel = (sock.connect_ex(("localhost", 27017)) != 0)

            if open_tunnel:
                ssh_tunnel = SSHTunnelForwarder(
                    ("da2.eecs.utk.edu", 9244),
                    ssh_username = "nwest13",
                    ssh_pkey = os.path.join(os.environ["HOME"], ".ssh", "da2.pem"),
                    remote_bind_address = ("da1.eecs.utk.edu", 27017),
                    local_bind_address = ("localhost", 27017)
                )

            hostname = "localhost"

    with ssh_tunnel, contextlib.closing(pymongo.MongoClient(hostname)) as conn:
        yield conn[dbname]

# Open a default collection (setting up indices and removing duplicates)
@contextlib.contextmanager
def opencoll(db, collname):
    """
    Opens a collection in the database. This does a little more than just
    subscripting the database. First, before yielding the collection, it sets
    up the indices of the collection, depending on the collection name. Then,
    before losing the collection, it removes any records with duplicate IDs it
    finds in the collection. Older records are prioritized for removal over
    newer ones.

    :param db pymongo.database.Database: Pymongo database containing collection
    :param collname str: Name of the collection to open
    :return: A context manager that yields the collection
    """

    coll = db[collname]

    index_tab = {
        re.compile(r"Labeled\w*"): [
            pymongo.IndexModel([('tags', pymongo.ASCENDING)], name = 'tags_index')
        ],
        re.compile(r"(Labeled)?Statuses_[a-zA-Z]+_A"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('user.id', pymongo.HASHED)], name = 'user_id_index'),
            pymongo.IndexModel([('user.screen_name', pymongo.HASHED)], name = 'user_screen_name_index'),
            pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english'),
            pymongo.IndexModel([('created_at', pymongo.ASCENDING)], name = 'created_at_index'),
            pymongo.IndexModel([('retrieved_at', pymongo.ASCENDING)], name = 'retrieved_at_index')
        ],
        re.compile(r"(Labeled)?Statuses_[a-zA-Z]+_C"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index', sparse = True),
            pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english', sparse = True)
        ],
        re.compile(r"Users_[a-zA-Z]+"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('screen_name', pymongo.HASHED)], name = 'screen_name_index'),
            pymongo.IndexModel([('description', pymongo.TEXT)], name = 'description_index'),
            pymongo.IndexModel([('created_at', pymongo.ASCENDING)], name = 'created_at_index'),
            pymongo.IndexModel([('retrieved_at', pymongo.ASCENDING)], name = 'retrieved_at_index')
        ],
        re.compile(r"Geolocations_[a-zA-Z]+"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('latitude', pymongo.ASCENDING), ('longitude', pymongo.ASCENDING)], name = 'latitude_longitude_index'),
            pymongo.IndexModel([('geojson', pymongo.GEOSPHERE)], name = 'geojson_index')
        ],
        re.compile(r"Images_[a-zA-Z]+"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('md5sum', pymongo.HASHED)], name = 'md5sum_index'),
            pymongo.IndexModel([('sha1sum', pymongo.HASHED)], name = 'sha1sum_index')
        ]
    }

    # Set up indices
    indices = sum((v for k, v in index_tab.items() if k.fullmatch(collname) is not None), [])

    if indices:
        coll.create_indexes(indices)

    index_names = {i["name"] for i in coll.list_indexes()}

    yield coll

    # Remove duplicates
    if "id_index" in index_names:
        dups = []
        ids = set()

        with contextlib.closing(coll.find(projection = ["id"], no_cursor_timeout = True)) as cursor:
            if "retrieved_at_index" in index_names:
                cursor = cursor.sort("retrieved_at", direction = pymongo.DESCENDING)

            for r in cursor:
                if 'id' in r:
                    if r['id'] in ids:
                        dups.append(r['_id'])

                    ids.add(r['id'])

        for i in range(0, len(dups), 800000):
            coll.delete_many({"_id": {"$in": dups[i:i + 800000]}})

def statusconv(status, *, status_permalink = None):
    """
    Convert tweets obtained with extended REST API to a format similar to the
    compatibility mode used by the streaming API. This is necessary to keep
    everything in a consistent format. (TODO: expand)

    :param status dict: Status to manipulate
    :param status_permalink str: Permalink to the status, not to be specified
    directly
    :return: A copy of the status that has been modified as described above
    """

    r = copy.deepcopy(status)

    if "extended_tweet" in r:
        return r

    full_text = r["full_text"]
    entities = r["entities"]

    r["extended_tweet"] = {
        "full_text": r["full_text"],
        "display_text_range": r["display_text_range"],
        "entities": r["entities"]
    }

    del r["full_text"]
    del r["display_text_range"]

    if "extended_entities" in r:
        r["extended_tweet"]["extended_entities"] = r["extended_entities"]
        del r["extended_entities"]

    if len(full_text) > 140:
        r["truncated"] = True

        if status_permalink is None:
            long_url = "https://twitter.com/tweet/web/status/" + r["id_str"]

            # Use TinyURL to shorten link to tweet
            while True:
                try:
                    with urlopen('http://tinyurl.com/api-create.php?' + urlencode({'url': long_url})) as response:
                        short_url = response.read().decode()
                    break
                except HTTPError:
                    time.sleep(5)

            status_permalink = {
                "url": short_url,
                "expanded_url": long_url,
                "display_url": "twitter.com/tweet/web/status/\u2026",
                "indices": [140 - len(short_url), 140]
            }
        else:
            short_url = status_permalink["url"]
            status_permalink["indices"] = [140 - len(short_url), 140]

        r["text"] = full_text[:(138 - len(short_url))] + "\u2026 " + short_url

        r["entities"] = {
            "hashtags": [],
            "symbols": [],
            "user_mentions": [],
            "urls": [status_permalink]
        }

        for k in r["entities"].keys():
            for v in entities[k]:
                if v["indices"][1] <= 138 - len(short_url):
                    r["entities"][k].append(v)

    else:
        r["text"] = full_text
        r["entities"] = {k: entities[k] for k in ("hashtags", "symbols", "user_mentions", "urls")}

    if "quoted_status" in r:
        if "quoted_status_permalink" in r:
            quoted_status_permalink = r["quoted_status_permalink"]
            del r["quoted_status_permalink"]
        else:
            quoted_status_permalink = None

        r["quoted_status"] = statusconv(r["quoted_status"], status_permalink = quoted_status_permalink)

    return r

# Convert RFC 2822 date strings in a status to datetime objects
def adddates(status, retrieved_at = None):
    """
    Converts RFC 2822 date strings in a status to datetime instancess in the
    status object. This is mainly a nicety; JSON doesn't have a date type, but
    BSON (which Pymongo uses internally) does, and Pymongo automatically
    converts them from/to datetime instances. It also creates the
    "retrieved_at" field for the status if the argument is specified.

    :param status dict: Status to manipulate
    :param retrieved_at datetime.datetime: Datetime instance representing when
    the status was retrieved, optional
    :return: A copy of the status that has been modified as described above
    """

    r = copy.deepcopy(status)

    r["created_at"] = parsedate_to_datetime(r["created_at"])
    r["user"]["created_at"] = parsedate_to_datetime(r["user"]["created_at"])

    if "quoted_status" in r:
        r["quoted_status"]["created_at"] = parsedate_to_datetime(r["quoted_status"]["created_at"])

    if retrieved_at is not None:
        r["retrieved_at"] = retrieved_at

    return r
