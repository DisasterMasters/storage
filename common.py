import contextlib
import copy
import datetime
from email.utils import parsedate_to_datetime
import io
import itertools
import json
import os
import re
import select
import socket
import socketserver
import threading
import time
import unicodedata
from urllib.error import HTTPError
from urllib.request import urlopen
from urllib.parse import urlencode

import paramiko
import pymongo
from fuzzywuzzy import process as fuzz_process
from nltk.corpus import stopwords

__all__ = [
    "TWITTER_AUTH",
    "TWITTER_AUTHKEY",
    "RUNNING_ON_DA2",
    "opentunnel",
    "opendb",
    "addindices",
    "rmdups",
    "opencoll",
    "statusconv",
    "adddates",
    "searchcoll"
]

try:
    from tweepy import OAuthHandler

    filename = os.environ.get("TWITTER_CREDENTIALS", os.path.join(os.environ["HOME"], "twitter_creds.json"))

    with open(filename, "r") as fd:
        creds = json.load(file)

        # Twitter API authentication token for this project
    TWITTER_CREDENTIALS = OAuthHandler(creds["consumer"], creds["consumer_secret"])
    TWITTER_AUTHKEY.set_access_token(creds["access_token"], creds["access_token_secret"])
except:
    TWITTER_AUTHKEY = None

try:
    from oauth2client.service_account import ServiceAccountCredentials

    filename = os.environ.get("GSPREAD_CREDENTIALS", os.path.join(os.environ["HOME"], "gspread_creds.json"))

    with open(filename, "r") as file:
        creds = json.load(file)

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]

    GSPREAD_CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name(creds, scope)
except:
    GSPREAD_CREDENTIALS = None

# To maintain backwards-compatibility
TWITTER_AUTHKEY = TWITTER_CREDENTIALS
TWITTER_AUTH = TWITTER_AUTHKEY

RUNNING_ON_DA2 = socket.gethostname() == "75f7e392a7ec"

try:
    NullContext = contextlib.nullcontext
except AttributeError: # Fix for Python <3.7
    class NullContext:
        def __init__(self):
            pass

        def __enter__(self):
            pass

        def __exit__(self, type, value, traceback):
            pass

class TunnelHandler(socketserver.BaseRequestHandler):
    def setup(self):
        self.transport = self.server.transport
        self.remote_hostname = self.server.remote_hostname
        self.remote_port = self.server.remote_port

    def handle(self):
        try:
            self.chan = self.transport.open_channel(
                "direct-tcpip",
                (self.remote_hostname, self.remote_port),
                self.request.getpeername(),
            )
        except:
            return

        if self.chan is None:
            return

        while True:
            r, _, _ = select.select([self.request, self.chan], [], [])

            if self.request in r:
                data = self.request.recv(4096)

                if len(data) == 0:
                    break

                self.chan.send(data)

            if self.chan in r:
                data = self.chan.recv(4096)

                if len(data) == 0:
                    break

                self.request.send(data)

    def finish(self):
        self.chan.close()
        self.request.close()

class LocalForwardServer(socketserver.ThreadingTCPServer, threading.Thread):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, transport, local_port, remote_hostname, remote_port):
        socketserver.ThreadingTCPServer.__init__(self, ("", local_port), TunnelHandler)
        threading.Thread.__init__(self)

        self.transport = transport
        self.remote_hostname = remote_hostname
        self.remote_port = remote_port

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, traceback):
        self.shutdown()
        self.join()

        self.server_close()

    def run(self):
        self.serve_forever()

class SFTPWrapper:
    # TODO: Add more functions here
    def open(self, *args, **kwargs):
        return open(*args, **kwargs)

    def mkdir(self, *args, **kwargs):
        return os.mkdir(*args, **kwargs)

@contextlib.contextmanager
def opentunnel(*, hostname = None, port = None, username = None, password = None, pkey = None):
    if hostname is None:
        hostname = "da2.eecs.utk.edu"

    if port is None:
        port = 9244

    if username is None:
        username = "nwest13"

    if isinstance(pkey, io.IOBase):
        pkey = paramiko.RSAKey.from_private_key(pkey)
    elif isinstance(pkey, str):
        pkey = paramiko.RSAKey.from_private_key_file(pkey)
    elif pkey is None:
        pkey = paramiko.RSAKey.from_private_key_file(os.path.join(os.environ["HOME"], ".ssh", "da2.pem"))

    #if not isinstance(pkey, paramiko.PKey) and password is None:
    #    raise TypeError

    with contextlib.closing(paramiko.Transport((hostname, port))) as conn:
        if password is not None:
            conn.connect(username = username, password = password)
        else:
            conn.connect(username = username, pkey = pkey)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            mongodb_isopen = (sock.connect_ex(("localhost", 27017)) == 0)
            sock.detach()
            jupyter_isopen = (sock.connect_ex(("localhost", 8889)) == 0)

        mongodb_fwd = NullContext() if mongodb_isopen else LocalForwardServer(conn, 27017, "da1.eecs.utk.edu", 27017)
        jupyter_fwd = NullContext() if jupyter_isopen else LocalForwardServer(conn, 8889, "localhost", 8888)

        with mongodb_fwd, jupyter_fwd, contextlib.closing(conn.open_sftp_client()) as sftp:
            yield sftp

'''
class MongoCollection(pymongo.Collection):
    def __enter__(self):
        index_tab = {
            re.compile(r".*?:.*?_labeled"): [
                pymongo.IndexModel([('tags', pymongo.ASCENDING)], name = 'tags_index')
            ],
            re.compile(r"statuses_.*?a.*?:.*"): [
                pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
                pymongo.IndexModel([('user.id', pymongo.HASHED)], name = 'user_id_index'),
                pymongo.IndexModel([('user.screen_name', pymongo.HASHED)], name = 'user_screen_name_index'),
                pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english'),
                pymongo.IndexModel([('created_at', pymongo.ASCENDING)], name = 'created_at_index'),
                pymongo.IndexModel([('retrieved_at', pymongo.ASCENDING)], name = 'retrieved_at_index')
            ],
            re.compile(r"statuses_.*?c.*?:.*"): [
                pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index', sparse = True),
                pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english', sparse = True)
            ],
            re.compile(r"statuses_.*?k.*?:.*"): [
                pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
                pymongo.IndexModel([('user.id', pymongo.HASHED)], name = 'user_id_index'),
                pymongo.IndexModel([('user.screen_name', pymongo.HASHED)], name = 'user_screen_name_index'),
                pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english')
            ],
            re.compile(r"users_.*?a.*?:.*"): [
                pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
                pymongo.IndexModel([('screen_name', pymongo.HASHED)], name = 'screen_name_index'),
                pymongo.IndexModel([('description', pymongo.TEXT)], name = 'description_index'),
                pymongo.IndexModel([('created_at', pymongo.ASCENDING)], name = 'created_at_index'),
                pymongo.IndexModel([('retrieved_at', pymongo.ASCENDING)], name = 'retrieved_at_index')
            ],
            re.compile(r"geolocations:.*"): [
                pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
                pymongo.IndexModel([('latitude', pymongo.ASCENDING), ('longitude', pymongo.ASCENDING)], name = 'latitude_longitude_index'),
                pymongo.IndexModel([('geojson', pymongo.GEOSPHERE)], name = 'geojson_index')
            ],
            re.compile(r"media:.*"): [
                pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
                pymongo.IndexModel([('retrieved_at', pymongo.ASCENDING)], name = 'retrieved_at_index'),
                pymongo.IndexModel([('media.retrieved_at', pymongo.ASCENDING)], name = 'media_retrieved_at_index'),
                pymongo.IndexModel([('media.local_url', pymongo.ASCENDING)], name = 'media_local_url_index')
            ]
        }

        # Set up indices
        indices = sum((v for k, v in index_tab.items() if k.fullmatch(collname) is not None), [])

        if indices:
            self.create_indexes(indices)

        return self

    def __exit__(self, type, value, tb):
        index_names = {i["name"] for i in self.list_indexes()}

        # Remove duplicates
        if "id_index" in index_names:
            dups = []
            ids = set()

            with contextlib.closing(self.find(projection = ["id"], no_cursor_timeout = True)) as cursor:
                if "retrieved_at_index" in index_names:
                    cursor = cursor.sort("retrieved_at", direction = pymongo.DESCENDING)

                for r in cursor:
                    if 'id' in r:
                        if r['id'] in ids:
                            dups.append(r['_id'])

                        ids.add(r['id'])

            for i in range(0, len(dups), 800000):
                coll.delete_many({"_id": {"$in": dups[i:i + 800000]}})

class MongoDatabase(pymongo.Database):
    def __getattr__(self, key):
        return MongoCollection(self, key)
'''

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

    if hostname is None:
        if os.environ.get("MONGODB_HOST") is not None:
            hostname = os.environ.get("MONGODB_HOST")
        elif RUNNING_ON_DA2:
            hostname = "da1.eecs.utk.edu"
        else:
            hostname = "localhost"

    with contextlib.closing(pymongo.MongoClient(hostname)) as conn:
        yield conn[dbname]


def addindices(coll):
    index_tab = {
        re.compile(r"[a-z_]*:[^_]*_labeled"): [
            pymongo.IndexModel([('tags', pymongo.ASCENDING)], name = 'tags_index')
        ],
        re.compile(r"statuses_[b-z]*a[b-z]*:.*"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('user.id', pymongo.HASHED)], name = 'user_id_index'),
            pymongo.IndexModel([('user.screen_name', pymongo.HASHED)], name = 'user_screen_name_index'),
            pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english'),
            pymongo.IndexModel([('lang', pymongo.ASCENDING)], name = 'lang_index'),
            pymongo.IndexModel([('created_at', pymongo.ASCENDING)], name = 'created_at_index'),
            pymongo.IndexModel([('retrieved_at', pymongo.ASCENDING)], name = 'retrieved_at_index')
        ],
        re.compile(r"statuses_[abd-z]*c[abd-z]*:.*"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index', sparse = True),
            pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english', sparse = True)
        ],
        re.compile(r"statuses_[a-jl-z]*k[a-jl-z]*:.*"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('user.id', pymongo.HASHED)], name = 'user_id_index'),
            pymongo.IndexModel([('user.screen_name', pymongo.HASHED)], name = 'user_screen_name_index'),
            pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english')
        ],
        re.compile(r"statuses_[a-rt-z]*s[a-rt-z]*:.*"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('user.id', pymongo.HASHED)], name = 'user_id_index'),
            pymongo.IndexModel([('user.screen_name', pymongo.HASHED)], name = 'user_screen_name_index'),
            pymongo.IndexModel([('text', pymongo.TEXT)], name = 'text_index', default_language = 'english')
        ],
        re.compile(r"users_[b-z]*a[b-z]*:.*"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('screen_name', pymongo.HASHED)], name = 'screen_name_index'),
            pymongo.IndexModel([('description', pymongo.TEXT)], name = 'description_index'),
            pymongo.IndexModel([('created_at', pymongo.ASCENDING)], name = 'created_at_index'),
            pymongo.IndexModel([('retrieved_at', pymongo.ASCENDING)], name = 'retrieved_at_index')
        ],
        re.compile(r"geolocations:.*"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('latitude', pymongo.ASCENDING), ('longitude', pymongo.ASCENDING)], name = 'latitude_longitude_index'),
            pymongo.IndexModel([('geojson', pymongo.GEOSPHERE)], name = 'geojson_index')
        ],
        re.compile(r"media:.*"): [
            pymongo.IndexModel([('id', pymongo.HASHED)], name = 'id_index'),
            pymongo.IndexModel([('retrieved_at', pymongo.ASCENDING)], name = 'retrieved_at_index'),
            pymongo.IndexModel([('media.retrieved_at', pymongo.ASCENDING)], name = 'media_retrieved_at_index'),
            pymongo.IndexModel([('media.local_url', pymongo.ASCENDING)], name = 'media_local_url_index')
        ]
    }

    indices = sum((v for k, v in index_tab.items() if k.fullmatch(collname) is not None), [])

    if indices:
        coll.create_indexes(indices)

def rmdups(coll):
    indices = dict(itertools.chain.from_iterable(i["key"].items() for i in coll.list_indices()))
    dups = []

    # Remove duplicates
    if indices.get("id") is not None:
        with contextlib.closing(coll.find(projection = ["id"], no_cursor_timeout = True)) as cursor:
            retrieved_at = indices.get("retrieved_at")

            if retrieved_at == pymongo.ASCENDING or retrieved_at == pymongo.DESCENDING:
                cursor = cursor.sort("retrieved_at", direction = pymongo.DESCENDING)

            ids = set()

            for r in cursor:
                if "id" in r:
                    if r["id"] in ids:
                        dups.append(r["_id"])

                    ids.add(r["id"])

        for i in range(0, len(dups), 800000):
            coll.delete_many({"_id": {"$in": dups[i:i + 800000]}})

    return len(dups)

# Open a default collection (setting up indices and removing duplicates)
@contextlib.contextmanager
def opencoll(db, collname, *, cleanup = True):
    """
    Opens a collection in the database. This does a little more than just
    subscripting the database. First, before yielding the collection, it sets
    up the indices of the collection, depending on the collection name. Then,
    before losing the collection, it removes any records with duplicate IDs it
    finds in the collection. Older records are prioritized for removal over
    newer ones.

    :param db pymongo.database.Database: Pymongo database containing collection
    :param collname str: Name of the collection to open
    :param cleanup bool: If True, attempts to delete duplicate records
    :return: A context manager that yields the collection
    """

    yield db[collname]

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
                except HTTPError:
                    time.sleep(5)
                else:
                    break

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

    if "retweeted_status" in r:
        try:
            r["retweeted_status"] = statusconv(r["retweeted_status"])
        except KeyError:
            pass

    return r

# Convert RFC 2822 date strings in a status to datetime objects
def adddates(status, retrieved_at = None):
    """
    Converts RFC 2822 date strings in a status to datetime instances in the
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
        r["quoted_status"]["user"]["created_at"] = parsedate_to_datetime(r["quoted_status"]["user"]["created_at"])

    if "retweeted_status" in r:
        r["retweeted_status"]["created_at"] = parsedate_to_datetime(r["retweeted_status"]["created_at"])
        r["retweeted_status"]["user"]["created_at"] = parsedate_to_datetime(r["retweeted_status"]["user"]["created_at"])

    if retrieved_at is not None:
        r["retrieved_at"] = retrieved_at

    return r

def getnicetext(r):
    try:
        text = r["extended_tweet"]["full_text"]
    except KeyError:
        try:
            text = r["text"][:getnicetext.regex.search(r["text"]).end()] + r["retweeted_status"]["extended_tweet"]["full_text"]
        except (KeyError, AttributeError):
            text = r["text"]

    return text

getnicetext.regex = re.compile(r"RT @[A-Za-z0-9_]{1,15}: ")

def searchcoll(coll, text, *args, **kwargs):
    clauses = []

    for match in IntelligentSearch.regex.finditer(" " + self.text + " "):
        clause = match.group().strip()

        if clause not in IntelligentSearch.stopwords:
            if " " in clause:
                clause = '"' + clause + '"'

            clauses.append(clause)

    mongo_query = {"$text": {"$search": ' '.join(clauses)}}

    with contextlib.closing(coll.find(mongo_query, *args, no_cursor_timeout = True, **kwargs)) as cursor:
        for r, score in fuzz_process.extractWithoutOrder({"text": text}, cursor, processor = lambda r: r["text"]):
            r["collection"] = coll.name
            r["score"] = score

            yield r

searchcoll.regex = re.compile(r" (\w+ )+|\w+")
searchcoll.stopwords = frozenset(stopwords.words('english') + ["http", "https", "www", "com", "net", "org"])

'''
def getcleantext(r):
    text = getnicetext(r)

    # Normalize Unicode
    cleantext = unicodedata.normalize("NFC", text)
    # Remove characters outside BMP (emojis)
    cleantext = "".join(c for c in cleantext if ord(c) <= 0xFFFF)
    # Remove newlines and tabs
    cleantext = cleantext.replace("\n", " ").replace("\t", " ")
    # Remove HTTP(S) link
    cleantext = re.sub(r"https?://\S+", "", cleantext)
    # Remove pic.twitter.com
    cleantext = re.sub(r"pic.twitter.com/\S+", "", cleantext)
    # Remove @handle at the start of the tweet
    cleantext = re.sub(r"\A(@[A-Za-z0-9_]{1,15} ?)*", "", cleantext)
    # Remove RT @handle:
    cleantext = re.sub(r"RT @[A-Za-z0-9_]{1,15}:", "", cleantext)
    # Strip whitespace
    cleantext = cleantext.strip()

    return cleantext
'''
