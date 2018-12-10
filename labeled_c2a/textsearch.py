import itertools

from fuzzywuzzy import process as fuzzy_match
import nltk

from common import *

def id_search(id, colls_in):
    if id is None:
        return None

    for coll in colls_in:
        r = coll.find_one({"id": id})

        if r is not None:
            return r

    return None

def text_search(text, colls_in):
    match_query = {"$text": {"$search": " ".join(nltk.tokenize.wordpunct_tokenize(text.strip().lower()))}}
    match_iter = itertools.chain.from_iterable(zip(itertools.repeat(i), coll.find(match_query, projection = ["text"])) for i, coll in enumerate(colls_in))

    match_map = {r["text"]: (i, r["_id"]) for i, r in match_iter}

    if len(match_map) == 0:
        return (None, 0)

    match, dist = fuzzy_match.extractOne(text, list(match_map.keys()))
    i, _id = match_map[match]

    return (colls_in[i].find_one({"_id": _id}), dist)

def gen_process(get_id_f, get_text_f, get_categories_f, api, colls_in):
    def process_f(r_in):
        id = get_id_f(r_in)
        text = get_text_f(r_in)
        categories = get_categories_f(r_in)

        if id is None:
            r_out, dist = text_search(text, colls_in)

            print(r_out["text"])

            if dist < 90:
                r_out = None
                err = "No suitable match found, closest was %d%%" % dist
        else:
            r_out = id_search(id, colls_in)

            if r_out is None:
                try:
                    r = api.get_status(
                        id,
                        tweet_mode = "extended",
                        include_entities = True,
                        monitor_rate_limit = True,
                        wait_on_rate_limit = True
                    )

                    retrieved_at = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

                except tweepy.TweepError as e:
                    r_out = None
                    err = "Twitter API error %d: %s" % (e.api_code, str(e))
                else:
                    r_out = adddates(statusconv(r), retrieved_at)

        if r_out is not None:
            if "_id" in r_out:
                del r_out["_id"]

            r_out["categories"] = categories

            print("\"%s\" -> \033[32mOk\033[0m (\"%s\", id: %s, categories: %s)" % (
                text,
                r_out["extended_tweet"]["full_text"],
                r_out["id_str"],
                ", ".join(str(cat) for cat in categories)
            ))
        else:
            print("\"%s\" -> \033[31mError\033[0m (%s)" % (text, err))

        return r_out

    return process_f
