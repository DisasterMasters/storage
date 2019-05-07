# Finding if a user is Local
It can be useful to determine if a twitter user is local to a specific area or not. For example, knowing if a news station is local to a city versus a national media company can help figure out if the information they post on twitter is more likely to pertain to a specific city or be national news. We have made a tool called ‘Locality.py’ that scrapes a user’s timeline for their specified location and tries to verify this by looking through 1000 of their followers and tallying up their locations. Then, it looks at the location with the most tallies and determines if it’s a city or state. If the user either had no location specified or the most tallied location doesn’t match the location on the user’s profile then that user is labeled not local.
The program can determine if it’s a city or a state because we have created a dictionary of cities and states that the program has access to. This dictionary is in ‘location_dict_better.txt’.

# PARAMETERS
```
def make_network(user_collection_name, network_collection_name, flag):
```
user_collection_name: the location the users are stored in and can be held in two ways.
                            -a .txt file of screen names (as long as we have these users in mongo)
                            -a mongodb collection of user objects
network_collection_name: the collection you want to store this network in the mongodb under twitter.
flag: pass 'txt' if you are getting users from a .txt file or 'col' if from a collection

# STORED FORMAT
```
{ 
        "_id" : ObjectId("5cad0b613339f635cd84ddab"),
        "id" : 14173315,
        "page_number" : 0,
        "followers_ids" : {
                "ids" : [
                        290709900,
                        NumberLong("1114590758248550401"),
                        NumberLong("812488235477856257"),
                        NumberLong("937027953120202752"),
                        481870959,
                        NumberLong("3284747288"),
                        NumberLong("1114126297087627264"),
                        NumberLong("1114853827952435200"),
                        NumberLong("765970307563843584")
                ],
                "next_cursor" : NumberLong("1630155681333552872"),
                "next_cursor_str" : "1630155681333552872",
                "previous_cursor" : 0,
                "previous_cursor_str" : "0",
                "total_count" : null
        },
        "following_ids" : null
}
```
# Storage Format Key
```
id: the user id
page_number: the number of the page returned by twitter
followers_ids: (5000 at most) list of user ids that follows a specific user. null if following is filled.
following ids: (5000 at most) list of user ids that a secific user follows. null if followers is filled.
other: used and populated by mongodb
```
