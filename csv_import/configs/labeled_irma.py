import csv

# Files/directories to get statuses from
SRCS = [
    "LABELED_DATA/utility_supervised_rf_sample_1000_10.19_ID & Manaul coding.csv"
    #"LABELED_DATA/utility_supervised_rf_sample_1000_10.19_ID & Manaul coding.csv",
    #"LABELED_DATA/manually_labelled_data.csv",
    #"LABELED_DATA/Manually_labelled_data_2.csv"
]

# Collection to import into
COLLNAME = "LabeledStatuses_Irma_C"

# Whether or not to use the sniffer
USE_SNIFFER = False

def PREPROCESS_FUNC(filename, row):
    return {
        "text": row["Tweet"].encode("ascii", errors = "ignore").decode(),
        "tags": [int(row["Manual Coding"]) if row["Manual Coding"] else 7]
    }
    '''
    elif "Label" in row:
        return {
            "id": int(row["Link"].split("/")[-1]),
            "text": row["Tweet"].encode("ascii", errors = "ignore").decode(),
            "tags": [int(row["Label"]) + 9]
        }
    '''

def GET_DIALECT_FIELDNAMES_FUNC(filename):
    return csv.excel
