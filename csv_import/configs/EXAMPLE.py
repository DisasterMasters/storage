import csv

# Files/directories to get statuses from
SRCS = [
    "DATA/florida_data"
]

# Collection to import into
COLLNAME = "Statuses_Irma_C"

# Whether or not to use the sniffer
USE_SNIFFER = False

# Function to preprocess records in the CSV file. The OrderedDict obtained from
# the csv.DictReader class is passed in, and ther return value should be a dict
# containing all the relevant information from the OrderedDict. If the data is
# invalid, None should be returned.
def PREPROCESS_FUNC(r):
    return dict(r)

# Function to return the Dialect to be used, given the filename. This is only
# called if USE_SNIFFER is false. If the file should be skipped, then None
# should be returned. Optionally, a second return value can be given to specify
# the header of the file
def GET_DIALECT_FIELDNAMES_FUNC(filename):
    return csv.excel
