import collections
import csv
import datetime
import re

# Files/directories to get statuses from
SRCS = [
    "LABELED_DATA/ClassifiedTweets.csv"
]

# Collection to import into
COLLNAME = "LabeledStatuses_Sandy_K"

# Whether or not to use the sniffer
USE_SNIFFER = False

geo_regex = re.compile(r"Point (?P<latitude>\-?\d+\.\d+) (?P<longitude>\-?\d+\.\d+)\|")

def PREPROCESS_FUNC(filename, row):
    match = geo_regex.fullmatch(row["Lat_long"])

    return {
        "id": int(row["TweetID"].strip("|\"")),
        "text": row["TweetBody"],
        "user": {"screen_name": row["UserID"]},
        "created_at": datetime.datetime.strptime(row["date_time"], "%m/%d/%Y %H:%M"),
        "coordinates": {"type": "Point", "coordinates": [float(match.group("longitude")), float(match.group("latitude"))]} if match else None,
        "tags": list(collections.OrderedDict.fromkeys([row["classification"], row["sentiment"]]).keys())
    }

def GET_DIALECT_FIELDNAMES_FUNC(filename):
    return csv.excel
