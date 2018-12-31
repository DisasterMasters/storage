# Collection to modify
COLLNAME = ""

# List of rules to transfer values from/to. Rules are in the form:
#
#   (from, to[, mapping])
#
# Where from and to represent the original and new keys, respectively, and
# mapping optionally performs any necessary transformations on the value
RULES = []

# Function to call on the record after applying the rules above
def POST_FUNC(r):
    return r
