Making User Networks
This function creates a mongodb collection of json user objects that consist of followers of a list of specific 
users and the users that a list of specific users follow.

PARAMETERS
```
def make_network(user_collection_name, network_collection_name, flag):
```
user_collection_name: the location the users are stored in and can be held in two ways.
                            -a .txt file of screen names (as long as we have these users in mongo)
                            -a mongodb collection of user objects
network_collection_name: the collection you want to store this network in the mongodb under twitter.
flag: pass 'txt' if you are getting users from a .txt file or 'col' if from a collection

STORED FORMAT
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
id: the user id
page_number: the number of the page returned by twitter
followers_ids: (5000 at most) list of user ids that follows a specific user. null if following is filled.
following ids: (5000 at most) list of user ids that a secific user follows. null if followers is filled.
other: used and populated by mongodb
