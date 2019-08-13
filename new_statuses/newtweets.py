import datetime
import threading
import time

import tweepy

from common import *

class QueueListener(tweepy.StreamListener):
    def __init__(self, qu, ev):
        #super().__init__(tweepy.API(parser = tweepy.parsers.JSONParser()))
        super().__init__()

        self.qu = qu
        self.ev = ev

    def on_status(self, status):
        retrieved_at = datetime.datetime.now(datetime.timezone.utc)
        r = adddates(status._json, retrieved_at)
        #r = adddates(status, retrieved_at)

        self.qu.put(r)

        return not self.ev.wait(0)

    def on_error(self, status_code):
        if self.ev.is_set():
            return False
        elif status_code == 420:
            time.sleep(60)
        elif status_code // 100 == 5:
            time.sleep(5)

class NewKeywordThread(threading.Thread):
    def __init__(self, queries, qu, ev):
        super().__init__()

        self.queries = queries

        self.strm = tweepy.Stream(auth = TWITTER_AUTH, listener = QueueListener(qu, ev))

    def run(self):
        while True:
            try:
                self.strm.filter(track = self.queries)
            except:
                time.sleep(5)
            else:
                break

class NewUsernameThread(threading.Thread):
    def __init__(self, queries, qu, ev):
        super().__init__()

        api = tweepy.API(TWITTER_AUTH)
        self.queries = [api.get_user(username).id_str for username in queries]

        self.strm = tweepy.Stream(auth = TWITTER_AUTH, listener = QueueListener(qu, ev))

    def run(self):
        while True:
            try:
                self.strm.filter(follow = self.queries)
            except:
                time.sleep(5)
            else:
                break

class NewLocationThread(threading.Thread):
    def __init__(self, queries, qu, ev):
        super().__init__()

        self.queries = [i for bbox in queries for i in bbox]
        assert len(self.queries) % 4 == 0

        self.strm = tweepy.Stream(auth = TWITTER_AUTH, listener = QueueListener(qu, ev))

    def run(self):
        while True:
            try:
                self.strm.filter(locations = self.queries)
            except:
                time.sleep(5)
            else:
                break
