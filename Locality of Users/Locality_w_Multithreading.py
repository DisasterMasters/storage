import difflib
from robobrowser import RoboBrowser
import requests
import random
from pathos.multiprocessing import ProcessingPool as Pool
import tweepy
import ast
from pymongo import MongoClient
from tqdm import tqdm
import sys

definitely_a_city = 0

#### NORMALIZES LOCATION IF IT'S A CITY FOLLOWED BY A COMMA ####
def city_comma_state_fixer(where, whip):
    #### STATE ABREVIATION TO STATE NAME DICTIONARY ####
    abrev_to_name = {'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
                     'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
                     'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
                     'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts',
                     'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana',
                     'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico',
                     'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
                     'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
                     'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
                     'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'}
    ####

    if ',' in where:
        words = where.split(',')
        if len(words) is 2:
            words[0] = words[0].strip()
            words[1] = words[1].strip()
            possible_state_word = words[1]
            if is_state(possible_state_word, 'US', whip) or is_state_abrev(possible_state_word, 'US', whip):
                # now find if words[0] is a city name so it won't get confused with a state name e.g. Delaware, OH
                if is_city(words[0], whip):
                    where = words[0]
                    print("TREATING " + where + " AS A CITY")
                else:
                    # check if where is a state abrev and change it to the full state name
                    if is_state_abrev(words[1], 'US', whip):
                        words[1] = abrev_to_name[words[1].upper()]
                    where = words[1]
                    print(words[0] + " IS NOT A CITY BUT " + where + " IS A STATE")
    return where
####

#### FINDS ALL LOCATIONS OF FOLLOWERS AND TALLIES UP SEEN LOCATIONS ####
def tallies(followers_ids, places_list):
    locations_tallied = {}
    followers_ids = followers_ids[:1000]

    p = Pool(5)
    locations_list = p.map(get_location, followers_ids)

    print("\nTALLYING UP LOCATIONS")
    for place in tqdm(locations_list):
        if place is not None and (place.isspace() is False) and place != "":
            place = city_comma_state_fixer(place)
            possible_places = difflib.get_close_matches(place, places_list, cutoff=.6)
            if possible_places is not None:
                if len(possible_places) >= 1:
                    #print("MATCHED PLACE: " + possible_places[0])
                    if possible_places[0] not in locations_tallied:
                        locations_tallied.update({possible_places[0]: 0})
                    locations_tallied[possible_places[0]] = locations_tallied[possible_places[0]] + 1
                    #print("TALLIES: " + str(locations[possible_places[0]]))
    return locations_tallied


#### GATHERS ALL STATES WITH SPECIFIED CITY ####
def states_with_city(s, country, whip):
    guesses = []
    for state in whip[country]['states']:
        for x in whip[country][state]:
            if x.lower() == s.lower():
                guesses.append(state)
    return guesses

#### CONFIRMS IF s IS A STATE ####
def is_state(s, country, whip):
    for x in whip[country]['states']:
        if x.lower() == s.lower():
            return True
    return False


#### CONFRIMS IF s IS A STATE ABREVIATION ####
def is_state_abrev(s, country, whip):
    for x in whip[country]['states_abrev']:
        if x == s.upper():
            return True
    return False

#### GATHERS ALL PLACES FROM WHIP DICTIONARY ####
def grab_all_places(whip):
    places_list = []
    for country, states_dict in whip.items():
        for state, places in states_dict.items():
            places_list.extend(places)
    return places_list


#### GATHERS ALL PLACES FROM WHIP DICTIONARY ####
def is_city(s, whip):
    global definitely_a_city
    cities_list = []
    for country, states_dict in whip.items():
        for state, places in states_dict.items():
            if state != 'states_abrev' and state != 'states':
                cities_list.extend(places)
    for x in cities_list:
        if x.lower() == s.lower():
            definitely_a_city = 1
            return True
    return False

#### GETS TWITTER USERS LOCATION FROM THEIR WEBPAGE ####
def get_location(id):
    HEADERS_LIST = [
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; x64; fr; rv:1.9.2.13) Gecko/20101203 Firebird/3.6.13',
        'Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201',
        'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16',
        'Mozilla/5.0 (Windows NT 5.2; RW; rv:7.0a1) Gecko/20091211 SeaMonkey/9.23a1pre'
    ]

    session = requests.Session()
    browser = RoboBrowser(session=session, user_agent=random.choice(HEADERS_LIST), parser='lxml')
    first_url = "https://twitter.com/intent/user?user_id=" + str(id)
    browser.open(first_url)
    results = browser.find_all("span", {"class": "nickname"})
    if results is not None and len(results) is not 0:
        handle = " ".join(str(results[0].text).split())
        url = "https://twitter.com/" + handle
        browser.open(url)
        results = browser.find_all("span", {"class": "ProfileHeaderCard-locationText u-dir"})
        if results is not None and len(results) is not 0:
            return " ".join(str(results[0].text).split())
    return None


#### DETERMINES LOCALITY ####
def Locality(user):
    global definitely_a_city
    #### TWITTER CREDENTIALS ####
    TWITTER_AUTH = tweepy.OAuthHandler(
        "ZFVyefAyg58PTdG7m8Mpe7cze",
        "KyWRZ9QkiC2MiscQ7aGpl5K2lbcR3pHYFTs7SCVIyxMlVfGjw0"
    )
    TWITTER_AUTH.set_access_token(
        "1041697847538638848-8J81uZBO1tMPvGHYXeVSngKuUz7Cyh",
        "jGNOVDxllHhO57EaN2FVejiR7crpENStbZ7bHqwv2tYDU"
    )

    api = tweepy.API(TWITTER_AUTH, parser=tweepy.parsers.JSONParser(), wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True, compression=True)
    ####

    #### IMPORTS CURRENT LOCATION DICTIONARY ####
    with open('location_dict_better.txt', 'r') as f:
        s = f.read()
        whip = ast.literal_eval(s)
    ####

    #### GETS 5000 OR LESS FOLLOWERS FROM USER WITH SPECIFIED ID ####
    followers_ids = []
    p = api.followers_ids(user['id'])
    followers_ids.extend(p['ids'])
    random.shuffle(followers_ids)
    ####

    places_list = grab_all_places(whip)

    locations = tallies(followers_ids, places_list)

    ### GETTING MOST TALLIED ####
    max_num = 0
    for key, value in locations.items():
        if value > max_num:
            max_num = value
            max_key = key
    ####

    #### USER'S SPECIFIED LOCATION ON TWITTER ####
    where = user['location']
    print("\nLOCATION ON " + sys.argv[1] + "'S TWITTER: " + where)
    # fix location on user's twitter is followed by a comma then a state
    where = city_comma_state_fixer(where)
    print("LOCATION ON " + sys.argv[1] + "'S TWITTER AFTER CITY OR STATE REMOVAL: " + where)
    ####

    #### DETERMINES WHERE USER IS LOCAL TO OR IF NOT LOCAL ####
    # returns 1 if local, 0 if not local, and -1 if no location
    source_results = difflib.get_close_matches(where, places_list, cutoff=.6)

    if len(source_results) is 0:
        return -1

    state_list_source = []
    state_list_guess = []

    if is_state(max_key, 'US', whip):
        state_list_guess.append(max_key)
        city_name_guess = None
    else:
        city_name_guess = max_key
        state_list_guess = states_with_city(max_key, 'US', whip)

    print("Guessed states: " + ', '.join(state_list_guess))
    if city_name_guess is None:
        print("Guessed city: None")
    else:
        print("Guessed city: " + city_name_guess)

    if is_state(source_results[0], 'US', whip) and definitely_a_city is 0:
        state_list_source.append(source_results[0])
        city_name_source = None
    else:
        city_name_source = source_results[0]
        state_list_source = states_with_city(source_results[0], 'US', whip)

    print("Source states: " + ', '.join(state_list_source))
    if city_name_source is None:
        print("Source city: None")
    else:
        print("Source city: " + city_name_source)

    if city_name_guess is not None and city_name_source is not None:
        if city_name_guess == city_name_source:
            return 1

    for state in state_list_guess:
        if state in state_list_source:
            return 1

    return 0
####

#### FINDS LOCALITY OF USER ####
if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 2:
        print("Usage -- \"python3 Locality_w_Multithreading 'username'\"")
        exit()

    client = MongoClient()  # server.local_bind_port is assigned local port
    all_users_names = set()
    all_users = []
    collection = client['twitter']['Users_Labeled']
    i = 0
    j = 0
    find_user = sys.argv[1]

    print("GRABBING ALL USERS")
    user_objects = collection.find()
    for user_object in tqdm(user_objects):
        if user_object['screen_name'] not in all_users_names:
            all_users.append(user_object)
            all_users_names.add(user_object['screen_name'])

    #### FINDING WHERE SPECIFIC USER IS IN ALL_USERS ####
    print("\nFINDING PLACE IN ALL USERS OF " + find_user)

    for user in all_users:
        if user["screen_name"] == find_user:
            j = 1
            break
        else:
            i += 1

    if j is 0:
        print(find_user + " wasn't found in the database")
        exit()
    else:
        where = i

    user = all_users[where]

    val = Locality(user)

    print(val)