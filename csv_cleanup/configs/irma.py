import datetime

COLLNAME = "Statuses_Irma_C"

# LOL WHY DO YOU NEED MULTI LINE LAMBDAS, YOU'RE STUPID HAHAHA
def date_to_datetime(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M")

RULES = [
    ("id", "id", int),
    ("text", "text"),
    ("date", "created_at", date_to_datetime),
    ("username", "user", lambda username: {"screen_name": username}),
    ("retweets", "retweet_count", int),
    ("favorites", "favorite_count", int),
    ("original_file", "original_file")
]

def POST_FUNC(r):
    return r
