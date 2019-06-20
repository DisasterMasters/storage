# Creating Test Sets for ML Algorithms
The program takes in a bunch of intital parameters based on what you want the test set to contain. After setting the initial parameters you run the first block and look at the print out.  From there you decide which column labels need to be used to access the data needed in the secondary parameters which you fill in. The program will then display what it is doing and progress bars along the way as well as the amount of remaining tweets in the subset as it restricts the constraints based on your parameters. after giving you the amount of tweets in the file it creates it saves the xlsx file and prints done.

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
