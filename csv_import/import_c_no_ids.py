import contextlib
import sys

from fuzzywuzzy import process as fuzzy_match
from nltk.tokenize import TweetTokenizer

from common import *

nltk_tokenizer = TweetTokenizer(preserve_case = False)

def text_search(text, colls):
    match_query = {"$text": {"$search": " ".join(nltk_tokenizer.tokenize(text.strip().lower()))}}
    match_map = {r["text"]: (i, r["_id"]) for i, coll in enumerate(colls) for r in coll.find(match_query, projection = ["text"])}

    if len(match_map) == 0:
        return None, -1

    match, dist = fuzzy_match.extractOne(text, list(match_map.keys()))
    i, _id = match_map[match]
    r = colls[i].find_one({"_id": _id})

    if r is None:
        return None, -1

    del r["_id"]

    return r, dist

if __name__ == "__main__":
    with contextlib.ExitStack() as exitstack:
        db = exitstack.enter_context(opendb())

        coll_in = exitstack.enter_context(opencoll(db, sys.argv[1]))
        colls_alltweets = [exitstack.enter_context(opencoll(db, collname)) for collname in sys.argv[2:-1]]
        coll_out = exitstack.enter_context(opencoll(db, sys.argv[-1]))

        cursor = exitstack.enter_context(contextlib.closing(coll_in.find(projection = ["text", "tags"], no_cursor_timeout = True)))

        for r in cursor:
            r_out, dist = text_search(r["text"], colls_alltweets)

            if dist < 90:
                print("\"%s\" -> \033[31mError\033[0m (No suitable match found, closest was %d%%)" % (r["text"], dist))
            else:
                r_out["tags"] = r["tags"]
                coll_out.insert_one(r_out)

                print("\"%s\" -> \033[32mOk\033[0m (\"%s\", id: %s, tags: %s)" % (
                    r["text"],
                    r_out["extended_tweet"]["full_text"],
                    r_out["id_str"],
                    ", ".join(map(str, r["tags"]))
                ))

'''
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

            r_out["tags"] = tags

            print("\"%s\" -> \033[32mOk\033[0m (\"%s\", id: %s, tags: %s)" % (
                text,
                r_out["extended_tweet"]["full_text"],
                r_out["id_str"],
                ", ".join(str(cat) for cat in categories)
            ))
        else:
            print("\"%s\" -> \033[31mError\033[0m (No suitable match found, closest was %d%%)" % (text, dist))

        return r_out

    return process_f
'''
