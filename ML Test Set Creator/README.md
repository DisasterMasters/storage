# FINDING IF A USER IS LOCAL
Scrapes a user’s timeline for their specified location and tries to verify this by looking through 1000 of their followers and tallying up their locations. Then, it looks at the location with the most tallies and determines if it’s a city or state. If the user either had no location specified or the most tallied location doesn’t match the location on the user’s profile then that user is labeled not local.
The program can determine if it’s a city or a state because we have created a dictionary of cities and states that the program has access to. This dictionary is in ‘location_dict_better.txt’.

# INITIAL PARAMETERS
```
#name of the collection on mongodb under twitter
collection_name = 'Tweets_Irma_Scraped'

#number of tweets to comprise the test set at most
num = 300

#the name of the csv you want to create
csv_name = 'announcement_tweets.xlsx'

#set to true if you want the test set to be comprised of only relevant tweets
remove_irrelevant = True

#set to true if you want the test set to only unique tweets
remove_duplicates = True

#set to true if you want the test set to not contain retweets
remove_retweets = True

#set to true if you want the test set to be comprised of only tweets in English
make_english_only = True

#set to true if you want the test set to be comprised of only tweets during the specified dates
make_only_in_time_frame = False
#date format '2019-01-01'
date_begin = '2017-09-01'
date_end = '2017-09-30'

#set to None if not needed
#otherwise set to list of keywords you want each tweet to contain one of ['governor', 'FEMA']
only_contain_keywords = ['governor', 'FEMA', 'fema', 'Fema']

#set to None if not needed
#otherwise set to the name of the csv that the training set is comprised of and that you dont want overlapping tweets from
training_set = '/home/nwest13/twitter/Alexa/Content Analysis/CA_MASTER.csv' #'opinion_new_train.csv'
```

# SECONDARY PARAMETERS
```
Date = 'datetime'  #the date of the tweet
Text = 'text'  #the tweets text
ID = 'ID'  #the tweets ID

#### For Training Set ####
Train_set_Text = 'text' #the tweets text 
```
