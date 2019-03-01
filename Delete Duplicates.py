
# coding: utf-8

# In[3]:


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

# Twitter API authentication token
TWITTER_AUTH = tweepy.OAuthHandler(
    "ZFVyefAyg58PTdG7m8Mpe7cze",
    "KyWRZ9QkiC2MiscQ7aGpl5K2lbcR3pHYFTs7SCVIyxMlVfGjw0"
)
TWITTER_AUTH.set_access_token(
    "1041697847538638848-8J81uZBO1tMPvGHYXeVSngKuUz7Cyh",
    "jGNOVDxllHhO57EaN2FVejiR7crpENStbZ7bHqwv2tYDU"
)

# Open a default connection
@contextlib.contextmanager
def opendb(hostname = None, dbname = "twitter"):
    if hostname is None:
        if os.environ.get("MONGODB_HOST") is not None:
            hostname = os.environ.get("MONGODB_HOST")
        elif socket.gethostname() == "75f7e392a7ec":
            hostname = "da1.eecs.utk.edu"
        else:
            hostname = "localhost"
    conn = pymongo.MongoClient(hostname)
    
    yield conn[dbname]


# In[7]:


import contextlib
from tqdm import tqdm

# Remove duplicates
def delete_duplicates(coll):
    index_names = {i["name"] for i in coll.list_indexes()}
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
                
    for i in tqdm(range(0, len(dups), 800000)):
        coll.delete_many({"_id": {"$in": dups[i:i + 800000]}})
        
with opendb() as db:
    delete_duplicates(db["Users_Labeled"])

