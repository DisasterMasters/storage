import collections
import contextlib
import math
import re
import pickle
import statistics
from email.utils import format_datetime
import csv

import nltk.corpus
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import TweetTokenizer
import pandas as pd
from numpy import nan as NaN

from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer

from common import *
from getcleantext import getcleantext

regex = re.compile(r"RT @[A-Za-z0-9_]{1,15}: ")

def get_full_text(r):
    try:
        text = r["extended_tweet"]["full_text"]
    except KeyError:
        try:
            text = r["text"][:regex.search(r["text"]).end()] + r["retweeted_status"]["extended_tweet"]["full_text"]
        except KeyError:
            text = r["text"]
        except AttributeError:
            text = r["text"]

    return text

import base64
import bson

def pack(r):
    return base64.b85encode(bson.BSON.encode(r)).decode()

def unpack(s):
    return bson.BSON.decode(base64.b85decode(s.encode()))

class d2v:
    def __init__(
        self,
        training_text,
        training_tags,
        tokenize_f = None,
        tags_to_train = None,
        tags_to_test = None,
        vector_size = 300,
        window_size = 15,
        min_count = 1,
        sampling_threshold = 1e-4,
        negative_size = 5,
        train_epoch = 40,
        dm = 0,
        worker_count = 7,
    ):
        training_data = list(zip(training_text, training_tags))

        if tokenize_f is None:
            tokenizer = TweetTokenizer(preserve_case = False)
            #stopwords = frozenset(nltk.corpus.stopwords.words("english"))
            #stemmer = SnowballStemmer("english")

            tokenize_f = tokenizer.tokenize

        if tags_to_train is None:
            tags_to_train = {tag for _, tags in training_data for tag in tags}

        if tags_to_test is None:
            tags_to_test = [tags_to_train]

        model = Doc2Vec(
            vector_size = vector_size,
            window_size = window_size,
            min_count = min_count,
            sampling_threshold = sampling_threshold,
            negative_size = negative_size,
            train_epoch = train_epoch,
            dm = dm,
            worker_count = worker_count
        )

        training_data_docs = []

        for text, tags in training_data:
            # Only add data points whose tags overlap with ys_to_train to the
            # training data set
            tags = list(set(tags) & tags_to_train)

            if tags:
                training_data_docs.append(TaggedDocument(tokenize_f(text), tags))

        model.build_vocab(training_data_docs)
        model.train(training_data_docs, total_examples = model.corpus_count, epochs = model.epochs)

        self.model = model
        self.tags = tags_to_test
        self.tokenize = tokenize_f

    def infer(self, text):
        vec = self.model.infer_vector(self.tokenize(text))
        sims = self.model.docvecs.most_similar([vec], topn = len(self.model.docvecs))

        ret = []

        for dset in self.tags:
            # Append the highest-rated element for each disjoint set
            a = [s for s in sims if s[0] in dset]

            if a:
                ret.append(max(a, key = lambda x: x[1])[0])

        return ret

    @staticmethod
    def test(df, x, y, *args, **kwargs):
        training_data, test_data = train_test_split(df, shuffle = True)

        model = d2v(training_data[x], training_data[y], *args, **kwargs)
        ys_to_test = set()

        for dset in model.tags:
            ys_to_test |= dset

        inferred_tags = test_data[x].map(model.infer)
        actual_tags = test_data[y].map(lambda y: list(set(y) & ys_to_test))

        mask = [bool(x) for x in actual_tags]
        inferred_tags = inferred_tags.where(mask).dropna()
        actual_tags = actual_tags.where(mask).dropna()

        mlb = MultiLabelBinarizer()
        inferred_mlb = mlb.fit_transform(inferred_tags)
        actual_mlb = mlb.fit_transform(actual_tags)

        f1 = f1_score(inferred_mlb, actual_mlb, average = None)
        acc = accuracy_score(inferred_mlb, actual_mlb)

        return acc, {k: v for k, v in zip(sorted(ys_to_test), f1)}

if __name__ == "__main__":
    '''
    stopwords = frozenset(nltk.corpus.stopwords.words("english"))
    stemmer = SnowballStemmer("english")

    regex_tokenizer = re.compile("\w+", re.I)
    nltk_tokenizer = TweetTokenizer(preserve_case = False)

    #regex = re.compile(r"RT @[A-Za-z0-9_]{1,15}: ")

    with opentunnel(), opendb() as db, opencoll(db, "LabeledStatuses_Power_A") as coll:
        rs = list(coll.find(projection = ["id", "text", "extended_tweet.full_text", "retweeted_status.extended_tweet.full_text", "tags"]))

    df = pd.DataFrame(rs)

    f1 = lambda x: x["full_text"]
    f2 = lambda x: x["extended_tweet"]["full_text"] if len(x) > 0 else NaN

    text_series = df["extended_tweet"].map(f1, na_action = "ignore")
    text_series.fillna(df["retweeted_status"].map(f2, na_action = "ignore"), inplace = True)
    text_series.fillna(df["text"], inplace = True)
    df["text"] = text_series
    df.drop(columns = ["_id", "retweeted_status", "extended_tweet"], inplace = True)

    print(df)

    methods = {
        "Using whitespace splitting, without any modifications": lambda x: x.split(),
        "Using whitespace splitting, with stemming": lambda x: [stemmer.stem(w) for w in x.split()],
        "Using whitespace splitting, with removal of stop words": lambda x: [w for w in x.split() if w not in stopwords],
        "Using whitespace splitting, with stemming and removal of stop words": lambda x: [stemmer.stem(w) for w in x.split() if w not in stopwords],
        "Using regexes, without any modifications": regex_tokenizer.findall,
        "Using regexes, with stemming": lambda x: [stemmer.stem(w) for w in regex_tokenizer.findall(x)],
        "Using regexes, with removal of stop words": lambda x: [w for w in regex_tokenizer.findall(x) if w not in stopwords],
        "Using regexes, with stemming and removal of stop words": lambda x: [stemmer.stem(w) for w in regex_tokenizer.findall(x) if w not in stopwords],
        "Using NLTK, without any modifications": nltk_tokenizer.tokenize,
        "Using NLTK, with stemming": lambda x: [stemmer.stem(w) for w in nltk_tokenizer.tokenize(x)],
        "Using NLTK, with removal of stop words": lambda x: [w for w in nltk_tokenizer.tokenize(x) if w not in stopwords],
        "Using NLTK, with stemming and removal of stop words": lambda x: [stemmer.stem(w) for w in nltk_tokenizer.tokenize(x) if w not in stopwords]
    }

    for name, method in methods.items():
        overall = []
        relevant = []
        irrelevant = []

        for _ in range(20):
            acc, f1 = d2v.test(df, "text", "tags", tokenize_f = method, tags_to_test = [{"relevant", "irrelevant"}])

            overall.append(acc)
            relevant.append(f1["relevant"])
            irrelevant.append(f1["irrelevant"])

        print(name + ":")
        print("\tOverall:    %f%% (Variance %f)" % (statistics.mean(overall), statistics.variance(overall)))
        print("\tRelevant:   %f%% (Variance %f)" % (statistics.mean(relevant), statistics.variance(relevant)))
        print("\tIrrelevant: %f%% (Variance %f)" % (statistics.mean(irrelevant), statistics.variance(irrelevant)))
        print()

    '''

    with opentunnel(), opendb() as db:
        labeled_text = []
        labeled_tags = []

        with opencoll(db, "LabeledStatuses_Power_A") as coll:
            for r in coll.find(projection = ["text", "extended_tweet.full_text", "retweeted_status.extended_tweet.full_text", "tags"]):
                try:
                    text = r["extended_tweet"]["full_text"]
                except KeyError:
                    try:
                        text = r["retweeted_status"]["extended_tweet"]["full_text"]
                    except KeyError:
                        text = r["text"]

                labeled_text.append(getcleantext(text))
                labeled_tags.append(r["tags"])

        model = d2v(labeled_text, labeled_tags, tags_to_test = [{"relevant", "irrelevant"}])

        with opencoll(db, "Statuses_MiscPower_A") as coll, open("relevant.csv", "w", newline = "", encoding = "utf-8") as fd:
            csvw = csv.writer(fd, quoting = csv.QUOTE_NONNUMERIC)
            csvw.writerow(["ID", "Text", "Username", "Date", "BSON85"])

            id_set = set()

            for r in coll.find():
                id = r["retweeted_status"]["id"] if "retweeted_status" in r else r["id"]

                if id in id_set:
                    continue

                id_set.add(id)

                try:
                    text = r["extended_tweet"]["full_text"]
                except KeyError:
                    try:
                        text = r["retweeted_status"]["extended_tweet"]["full_text"]
                    except KeyError:
                        text = r["text"]

                text = getcleantext(text)
                tags = model.infer(text)

                if text and "relevant" in tags:
                    csvw.writerow([
                        r["id"],
                        get_full_text(r),
                        r["user"]["screen_name"],
                        format_datetime(r["created_at"]),
                        pack(r)
                    ])
