import pandas
from pymongo import MongoClient
import sys
import os

from tqdm import tqdm

'''
from dateutil import parser
from datetime import datetime
import warnings
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
import sys
if '/home/nwest13/twitter/Zach/Tool_Box/Modules' not in sys.path:
    sys.path.insert(0, '/home/nwest13/twitter/Zach/Tool_Box/Modules')
from Text_Cleaner import tc

tc_class = tc()
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
client = gspread.authorize(creds)

coder1_sheet = client.open("Superior Manual Coding (SMC)").worksheet("coder1")
coder2_sheet = client.open("Superior Manual Coding (SMC)").worksheet("coder2")
coder3_sheet = client.open("Superior Manual Coding (SMC)").worksheet("coder3")

warnings.filterwarnings("ignore")

def conv_date_seconds(string):
    date = parser.parse(string)
    str_date = date.strftime("%Y-%m-%d")
    return int((datetime.strptime(str_date, "%Y-%m-%d")).timestamp())

#client = MongoClient('da1.eecs.utk.edu')
#db = client['twitter']

not_found = 0
'''
from Text_Cleaner import tc

import contextlib
import re
import pickle
from fuzzywuzzy import process as fuzzy_process
import multiprocessing as mp
import pymongo.errors
import shelve
import tempfile

regex = re.compile(r"\w+")

colls = [
    "Statuses_Florence_A",
    "Statuses_Irma_A",
    "Statuses_Maria_A",
    "Statuses_Irma_K",
    "Statuses_Irma_K_26_media_subset",
    "Statuses_Florence_C",
    "Statuses_Irma_C",
    "Statuses_Maria_C",
    "Statuses_Irma_K_w_usertype",
    "Statuses_Irma_Scraped_Streamed",
    "Statuses_Irma_Scraped_Streamed_Backup",
    "Statuses_Irma_YSP_Content",
    "Tweets_Barry_Scraped",
    "Tweets_Harvey_Scraped",
    "Tweets_Harvey_Scraped_Subset",
    "Tweets_Irma_Scraped"
]

class TextSearch:
    def __init__(self, query):
        self.query = query

    def __iter__(self):
        for coll in colls:
            with contextlib.closing(coll.find({"$text": {"$search": " ".join(regex.findall(self.query))}}, projection = ["ID", "id", "text"], no_cursor_timeout = True)) as cursor:
                for r in cursor:
                    if "id" not in r and "ID" in r:
                        r["id"] = r["ID"]
                        del r["ID"]

                    r["collection"] = coll.name
                    yield r

def initialize():
    global colls

    db = MongoClient('da1.eecs.utk.edu')['twitter']
    colls[:] = [db[coll] for coll in colls]

def fuzzymatch(indexrow):
    index, row = indexrow

    query = row['Text']
    match, dist = fuzzy_process.extractOne({"text": query}, TextSearch(query), processor = lambda r: r["text"])

    if dist < 90:
        match = None

    return index, row, match

with mp.Pool(len(os.sched_getaffinity(0)), initialize) as pool, shelve.open("matches") as db:
    df = pandas.read_excel(sys.argv[1], sheet_name="THE SHEET")
    # setup for filling
    df = df[['Order', 'Tweet', 'Code 1', 'Code 2']]
    df.rename(columns={'Code 1': 'Code_1', 'Code 2': 'Code_2', 'Tweet': 'Text'}, inplace=True)
    df.drop_duplicates(subset='Text', inplace=True)
    df['Collection'] = None
    df['Object_ID'] = None
    df['Tweet_id'] = None
    df['Date'] = None
    df['Sentiment'] = None
    df['Emotion'] = None
    df['Relevance'] = None
    df['Opinion'] = None
    df['DISCUSS'] = None
    df['REVIEWED'] = None
    df['found'] = None
    df['Cleaned_Text'] = None
    df['ct_size'] = None
    df['Match_Text'] = None

    df.dropna(subset=['Text'], inplace=True)
    df['Text'] = df['Text'].apply(lambda x: str(x))

    tc_class = tc()

    for i in df.index:
        df.at[i, 'Cleaned_Text'] = tc_class.clean(df.at[i, 'Text'], ['html_link', 'pic_link', 'english', 'retweet', 'dot', 'at_user', 'hashtag', 'lower', 'numbers', 'punc', 'stop_words', 'new_line'])
        df.at[i, 'ct_size'] = len((df.at[i, 'Cleaned_Text']).split())

    with tqdm(total=df.shape[0]) as pbar:
        matched = 0
        total = 0

        rows = dict(df.iterrows())

        for k, v in db.items():
            i = int(k)
            total += 1

            if v is not None:
                match = v
                matched += 1

                df.at[i, 'Collection'] =  match["collection"]
                df.at[i, 'Object_ID'] =  match['_id']
                df.at[i, 'Tweet_id'] =  match["id"]
                #df.at[i, 'Date'] = conv_date_seconds(ret['datetime'])
                df.at[i, 'found'] = 1
                df.at[i, 'Match_Text'] = match["text"]

            del rows[i]
            pbar.update()

        for i, row, match in pool.imap_unordered(fuzzymatch, rows.items()):
            total += 1

            if match is not None:
                matched += 1

                df.at[i, 'Collection'] =  match["collection"]
                df.at[i, 'Object_ID'] =  match['_id']
                df.at[i, 'Tweet_id'] =  match["id"]
                #df.at[i, 'Date'] = conv_date_seconds(ret['datetime'])
                df.at[i, 'found'] = 1
                df.at[i, 'Match_Text'] = match["text"]

            db[str(i)] = match
            pbar.update()

    print("Found a match for %d of %d rows" % (matched, total))

'''
# df contains tweets that were found and are ready to upload
#df = df[df['found'] == 1]
df = df[df['ct_size'] > 2]
print(not_found, "tweets not found")
gd.set_with_dataframe(coder1_sheet, pandas.DataFrame(df, columns=['Text', 'Code_1', 'Code_2', 'Relevance', 'Sentiment', 'Emotion', 'Opinion', 'DISCUSS', 'REVIEWED', 'Date', 'Collection', 'Object_ID', 'Tweet_id', 'Cleaned_Text']))
gd.set_with_dataframe(coder2_sheet, pandas.DataFrame(df, columns=['Text', 'Code_1', 'Code_2', 'Relevance', 'Sentiment', 'Emotion', 'Opinion', 'DISCUSS', 'REVIEWED', 'Date', 'Collection', 'Object_ID', 'Tweet_id', 'Cleaned_Text']))
gd.set_with_dataframe(coder3_sheet, pandas.DataFrame(df, columns=['Text', 'Code_1', 'Code_2', 'Relevance', 'Sentiment', 'Emotion', 'Opinion', 'DISCUSS', 'REVIEWED', 'Date', 'Collection', 'Object_ID', 'Tweet_id', 'Cleaned_Text']))
'''

with open(sys.argv[1][:sys.argv[1].rfind(".")] + ".pickle", "wb") as file:
    pickle.dump(df, file)


