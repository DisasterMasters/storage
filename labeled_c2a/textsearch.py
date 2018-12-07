from fuzzywuzzy import process as fuzzy_match
import nltk

def id_search(colls_in, id):
    for coll in colls_in:
        r = coll.find_one({"id": id})

        if r is not None:
            return r

    return None

def text_search(colls_in, text):
    match_query = {"$text": {"$search": " ".join(nltk.tokenize.wordpunct_tokenize(text.strip().lower()))}}
    match_iter = itertools.chain.from_iterable(zip(itertools.repeat(i), coll.find(match_query, projection = ["text"])) for i, coll in enumerate(colls_in))

    match_map = {r["text"]: (i, r["_id"]) for i, r in match_iter}

    match, dist = fuzzy_match.extractOne(text, list(match_map.keys()))

    i, _id = match_map[match]
    r = colls_in[i].find({"_id": _id})

    return (r, dist)
