import copy
from email.utils import parsedate_to_datetime

COLLNAME = "LabeledStatuses_MiscTechCompanies_C"

RULES = [
    ("Link", "id", lambda link: int(link.split("/")[-1])),
    ("Tweet", "text"),
    ("relevance", "categories", lambda relevance: [int(relevance)]),
    ("original_file", "original_file"),
]

def POST_FUNC(r):
    return r
