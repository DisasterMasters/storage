import copy
from email.utils import parsedate_to_datetime

COLLNAME = "LabeledStatuses_MiscTechCompanies_C"

RULES = [
    ("TweetId", "id", int),
    ("TweetText", "text"),
    ("TweetDate", "created_at", parsedate_to_datetime),
    ("original_file", "original_file"),
    ("Topic", "topic"),
    ("sentiment", "sentiment")
]

def POST_FUNC(r):
    r = copy.copy(r)

    r["categories"] = [r["topic"], r["sentiment"]]

    del r["topic"]
    del r["sentiment"]

    return r
