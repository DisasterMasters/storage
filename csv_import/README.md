# csv_slurp

These scripts attempt to add tweets in CSV and similarly formatted files to the MongoDB database. Essentially, this uses Python's `csv.DictReader` to parse a CSV file, adding each record to a collection.

## import_c.py
Reads a file or directory of CSV files, adding them to the specified MongoDB database. Requires a configuration script, see the configs folder for some examples.

## import_c_to_a.py
Re-retrieves all tweets in a certain collection with the Twitter API, placing the results in a different collection. This is useful because it may give us access to more information than was stored in the CSV file.

## import_a_to_users.py
Retrieves all users referenced in a certain collection (that was previously created with import_c_to_a.py), placing the results in a different collection.

## LABELED_DATA
Contains labeled data in the original format that it was given to me. Kept here for archival/reference purposes.
