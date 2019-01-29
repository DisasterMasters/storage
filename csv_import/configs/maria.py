import csv
import datetime
import re

class JulianDialect(csv.Dialect):
    delimiter = "|"
    quotechar = "'"
    doublequote = True
    skipinitialspace = False
    lineterminator = "\n"
    quoting = csv.QUOTE_NONE

SRCS = [
    "DATA/puertorico_data"
]

COLLNAME = "Statuses_Maria_C"

USE_SNIFFER = False

def PREPROCESS_FUNC(filename, row):
    row = {k.strip("'"): v for k, v in row.items() if k is not None}

    if "ID" in row and re.fullmatch(r"[0-9]+", row["ID"]):
        id_str = row["ID"]
    elif "permalink" in row and re.fullmatch(r"https?://\S+", row["permalink"]):
        id_str = row["permalink"].split("/")[-1]
    else:
        return None

    return {
        "id": int(id_str),
        "text": row["text"].replace("__NEWLINE__", "\n").replace("__PIPE__", "|"),
        "user": {"screen_name": row["username"]},
        "created_at": datetime.datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S"),
        "favorite_count": int(row["favorites"]),
        "retweet_count": int(row["retweets"])
    }

def GET_DIALECT_FIELDNAMES_FUNC(filename):
    if filename[filename.rfind('.'):] == ".log":
        return None

    return JulianDialect
