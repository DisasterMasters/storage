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
    "DATA/florida_data",
    "TweetAnalysis-579654ea3f7413313f11a42e5e4287a7bc6098ba/florida",
    "FloridaTweets"
]

COLLNAME = "Statuses_Irma_C"

USE_SNIFFER = False

def PREPROCESS_FUNC(filename, row):
    if "'ID'" in row and row["'ID'"]:
        return {
            "id": int(row["'ID'"]),
            "text": row["'text'"].replace("__NEWLINE__", "\n").replace("__PIPE__", "|"),
            "user": {"screen_name": row["'username'"]},
            "created_at": datetime.datetime.strptime(row["'date'"], "%Y-%m-%d %H:%M:%S"),
            "favorite_count": int(row["'favorites'"]),
            "retweet_count": int(row["'retweets'"])
        }
    elif "id" in row and row["id"]:
        if re.fullmatch(r"[0-9]+", row["id"]):
            id_str = row["id"]
        elif re.match(r"https?://\S+", row["permalink"]):
            id_str = row["permalink"].split("/")[-1]
        elif None in row:
            assert isinstance(row[None], list)
            match = re.search(r"[0-9]{15,}", "\003".join(row[None]))

            if match:
                id_str = match.group()
            else:
                return None
        else:
            return None

        return {
            "id": int(id_str),
            "text": row["text"],
            "user": {"screen_name": row["username"]},
            "created_at": datetime.datetime.strptime(row["date"], "%Y-%m-%d %H:%M"),
            "favorite_count": int(row["favorites"]),
            "retweet_count": int(row["retweets"]),
            "entities": {
                "hashtags": [{"text": hashtag.strip("#")} for hashtag in row["hashtags"].split()],
                "user_mentions": [{"screen_name": username.strip("@")} for username in row["mentions"].split()]
            }
        }

def GET_DIALECT_FIELDNAMES_FUNC(filename):
    ext = filename[filename.rfind('.'):]

    if ext == ".csv":
        with open(filename, "r", newline = '') as fd:
            blk = fd.read(16384)

        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(blk)

        if csv.Sniffer().has_header(blk):
            fieldnames = None
        else:
            fieldnames = "username,date,retweets,favorites,text,geo,mentions,hashtags,id,permalink,FixedSpaceIssues".split(",")

        return (dialect, fieldnames)
    elif ext == ".txt":
        return JulianDialect
    else:
        return None
