COLLNAME = "LabeledStatuses_Irma_C"

RULES = [
    ("Tweet", "text"),
    ("Manual Coding", "categories", lambda code: [int(code)]),
    ("original_file", "original_file")
]

def POST_FUNC(r):
    return r
