# csv_slurp

These scripts attempt to add tweets to the MmongoDB database in a less elegant manner than csv_import. Instead of trying to process the CSV files, they perform a regular expression on the entire file. This may lead to false positives, but it is useful in cases where the data files are corrupted or not properly formatted.

## slurp_users.py
Finds all mentions of Twitter user handles in a file, retrieves their information with the Twitter API, and places them in the specified collection.

## slurp_statuses.py
Finds all Twitter status IDs in a file, retrieves their information with the Twitter API, and places them in the specified collection.
