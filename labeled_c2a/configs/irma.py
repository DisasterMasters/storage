# Collection to save to
COLLNAME = "LabeledStatuses_Irma_A"

# CSV files to get labeled data from
FILES_STATUSES_C = []

# Collections to get labeled data from
COLLNAME_STATUSES_C = ["LabeledStatuses_Irma_C"]

# Collections to get new statuses from
COLLNAME_STATUSES_A = ["Statuses_Irma_A"]

# csv.Dialect of the CSV files in FILES_STATUSES_C. If this is None, then the
# csv.Sniffer class is used
CSV_DIALECT_OVERRIDE = None

# Function to a valid status ID from each old status in the collection. If this
# returns None, then fuzzy string matching will be used to find the original
# status (warning: slow)
def GET_ID_FIELD(r):
    return None

# Function to return the text body from each old status in the collection. Only
# used if the function above returns None
def GET_TEXT_FIELD(r):
    return r["Tweet"]

# Return the categories of the new labeled data
def GET_CATEGORIES_FIELD(r):
    return [int(r["Manual Coding"])]
