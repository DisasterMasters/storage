import collections
import csv
import datetime
import re

# Files/directories to get statuses from
SRCS = [
    "LABELED_DATA/987 FL local media tweets_sentiment manual coding_1.31_with updated categories - Final.csv"
]

# Collection to import into
COLLNAME = "LabeledStatuses_IrmaXiaojing_C"

# Whether or not to use the sniffer
USE_SNIFFER = False

def PREPROCESS_FUNC(filename, row):
    if all(not row[k] for k in ("Opinion", "Emotion class", "Sentiment", "Sarcasm")):
        return None

    emotionclass_map = {
        "A": "anger",
        "AS": "astonishment_surprise",
		"C": "confusion",
        "D": "disappointment",
        "DG": "disgust",
        "DP": "desperation",
        "G": "grief",
        "HJ": "happy_joy",
        "HP": "hopeful_anticipation",
        "I": "insult",
        "R": "relief",
        "S": "sad",
        "SH": "shame",
        "SP": "sympathy",
        "T": "thankful",
        "WF": "worry_fear"
    }

    sentiment_map = {
        "Ps": "positive",
        "Ng": "negative",
        "Nt": "neutral"
    }

    tags = []

    if row["Opinion"]:
        assert row["Opinion"] == "1"
        tags.append("opinion")

    if row["Emotion class"]:
        tags.append(emotionclass_map[row["Emotion class"]])

    if row["Sentiment"]:
        tags.append(sentiment_map[row["Sentiment"]])

    if row["Sarcasm"]:
        assert row["Sarcasm"] == "1"
        tags.append("sarcasm")

    assert tags

    return {
        "text": row["Tweet"],
        "created_at": datetime.datetime.strptime(row["Date"], "%d-%b-%y"),
        "tags": tags
    }

def GET_DIALECT_FIELDNAMES_FUNC(filename):
    return csv.excel
