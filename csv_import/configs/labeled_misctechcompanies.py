import csv
from email.utils import parsedate_to_datetime

# Files/directories to get statuses from
SRCS = [
    "LABELED_DATA/full-corpus.csv"
]

# Collection to import into
COLLNAME = "LabeledStatuses_MiscTechCompanies_C"

# Whether or not to use the sniffer
USE_SNIFFER = False

def PREPROCESS_FUNC(filename, row):
    return {
        "id": int(row["TweetId"]),
        "text": row["TweetText"],
        "created_at": parsedate_to_datetime(row["TweetDate"]),
        "tags": [row["Topic"], row["Sentiment"]]
    }

def GET_DIALECT_FIELDNAMES_FUNC(filename):
    class ThisDialect(csv.excel):
        quoting = csv.QUOTE_ALL

    return ThisDialect
