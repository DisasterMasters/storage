#Makes a network of users following a specified user and users a specified user follows.
#user_collection_name is the location the users are stored in. 
        #if can be held in two ways.
                #a .txt file of screen names (as long as we have these users in mongo)
                #a mongodb collection of user objects
#network_collection_name is the collection you want to store this network in the mongodb under twitter
#flag pass 'txt' if you are getting users from a .txt file or 'col' if from a collection
def make_network(user_collection_name, network_collection_name, flag):
    import tweepy
    from tweepy import OAuthHandler
    import fileinput
    from tqdm import tqdm
    from sshtunnel import SSHTunnelForwarder
    from pymongo import MongoClient
    import pymongo
    import pandas as pd

    client = MongoClient('da1.eecs.utk.edu') #server.local_bind_port is assigned local port
    tmp_lusers = [] #when using a .txt file this stores all the user screen_names
    lusers = [] #a list of user json objects

    ### makes a list of users ###
    
    #when grabing from a txt file populate a list of user screen_names and search the mongodb
            #for users and grab their json object and store in list
    if flag == 'txt':
        print('Grabbing users from txt file')
        with open(user_collection_name) as inp:
            tmp_lusers = inp.read().splitlines()
        coll_list = client['twitter'].collection_names()
        for coll in tqdm(coll_list):
            print('Matching with users in collection: ' + coll)
            if 'Users' in coll:
                for document in client['twitter'][coll].find():
                    for luser in tmp_lusers:
                        if document['screen_name'] == luser:
                            lusers.append(luser)
    #when grabing from a collection populate a list of user json objects
    elif flag == 'col':  
        print('Grabbing users from collection')
        collection = client['twitter'][user_collection_name]
        for user in tqdm(collection.find()):
            lusers.append(user)
    #error msg
    else:
        print('flag must be either txt or col')
        
    #where you want the followers and following to go
    network_collection = client['twitter'][network_collection_name] 

    print('# of Users to make network' + len(lusers), end='')


    #make a pandas df from user objects
    df = pd.DataFrame(lusers)
    df = df.applymap(str)
    print(df.columns.values)
    
    #keys for twitter api
    TWITTER_AUTH = tweepy.OAuthHandler(
    "ZFVyefAyg58PTdG7m8Mpe7cze",
    "KyWRZ9QkiC2MiscQ7aGpl5K2lbcR3pHYFTs7SCVIyxMlVfGjw0"
    )
    TWITTER_AUTH.set_access_token(
    "1041697847538638848-8J81uZBO1tMPvGHYXeVSngKuUz7Cyh",
    "jGNOVDxllHhO57EaN2FVejiR7crpENStbZ7bHqwv2tYDU"
    )
    
    #set the tweepy api
    api = tweepy.API(TWITTER_AUTH, parser = tweepy.parsers.JSONParser(), wait_on_rate_limit = True, wait_on_rate_limit_notify = True, compression=True)

    #grab one page at a time of followers (5k user ids) and add this list to a dictionary 
            #along with the user id and page number and folling set to none add this to mongo 
            #then loop untill you have all of the followers
    #then grab one page at a time of follwing (5k user ids) and add this list to a dictionary
            #along with the user id and page number and followers set to none add this to mongo
            #then loop untill you have all of the following
    i = 0
    j = 0
    k = 0
    for user in tqdm(lusers):        
        if network_collection.count_documents({"id": luser["id"]}) > 0:
            continue 
        i += 1
        j = 0
        k = 0

        print('# ' + str(i) + "  Getting Followers for " + user['screen_name'])
        try:
            for p in tweepy.Cursor(api.followers_ids, user['id']).pages():
                tmp_user = dict()
                tmp_user['id'] = user['id']
                print('Followers: # Total API Calls: ' + str(j))
                tmp_user['page_number'] = j
                j += 1
                tmp_user['followers_ids'] = p
                tmp_user['following_ids'] = None
                network_collection.insert_one(tmp_user)
        except tweepy.TweepError as e:
            print(e)
            pass

        print('# ' + str(i) + "  Getting Following for " + user['screen_name'])
        try:
            for p in tweepy.Cursor(api.friends_ids, user['id']).pages():
                tmp_user = dict()
                tmp_user['id'] = user['id']
                print('Following: # Total API Calls: ' + str(k))
                tmp_user['page_number'] = k
                k += 1
                tmp_user['following_ids'] = p
                tmp_user['followers_ids'] = None
                network_collection.insert_one(tmp_user)
        except tweepy.TweepError as e:
            print(e)
            pass
