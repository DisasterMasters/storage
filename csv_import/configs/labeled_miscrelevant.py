import csv

# Files/directories to get statuses from
SRCS = [
    "LABELED_DATA/2012_Sandy_Hurricane-ontopic_offtopic.csv",
    "LABELED_DATA/manually_labelled_data.csv",
    "LABELED_DATA/Manually_labelled_data_2.csv"
]

# Collection to import into
COLLNAME = "LabeledStatuses_MiscRelevant_C"

# Whether or not to use the sniffer
USE_SNIFFER = False

def PREPROCESS_FUNC(filename, row):
    return {
        "id": int(row["Link"].split("/")[-1] if "/" in row["Link"] else row["Link"].strip("'")),
        "text": row["Tweet"].encode("ascii", errors = "ignore").decode(),
        "tags": [int(row["Label"])]
    }

def GET_DIALECT_FIELDNAMES_FUNC(filename):
    return csv.excel
