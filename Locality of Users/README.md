# FINDING IF A USER IS LOCAL
Scrapes a user’s timeline for their specified location and tries to verify this by looking through 1000 of their followers and tallying up their locations. Then, it looks at the location with the most tallies and determines if it’s a city or state. If the user either had no location specified or the most tallied location doesn’t match the location on the user’s profile then that user is labeled not local.
The program can determine if it’s a city or a state because we have created a dictionary of cities and states that the program has access to. This dictionary is in ‘location_dict_better.txt’.

# PARAMETERS
```
def Locality(user):
```
user: a tweepy user json object

# IMPORTS
```
location_dict_better.txt
```
.txt: holding all the counties, cities, states, and countries 
