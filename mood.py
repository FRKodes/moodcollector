from datetime import date
from datetime import datetime
from datetime import timedelta

from const import SIMPLE_GEO_TOKEN, SIMPLE_GEO_SECRET, SIMPLE_GEO_LAYER
from const import SLEEP_TIME, TWITTER_SEARCH_URL

from django.utils import simplejson
from simplegeo import Record, Client

import time
import sys
import urllib2

HAPPY_WORDS = ["happy", "blessed", "blest", "blissful", "blithe",
    "captivated", "cheerful", "chipper", "chirpy", "content", "contented",
    "convivial", "delighted", "ecstatic", "elated", "exultant", "glad",
    "gleeful", "gratified", "intoxicated", "jolly", "joyful", "joyous",
    "jubilant", "laughing", "light", "lively", "merry", "mirthful",
    "overjoyed", "peaceful", "peppy", "perky", "playful", "pleasant", "pleased",
    "sparkling", "sunny", "thrilled", "tickled", "upbeat" ]

SAD_WORDS = ["sad", "bereaved", "bitter", "cheerless", "dejected", "despairing",
    "despondent", "disconsolate", "dismal", "distressed", "doleful", "down",
    "downcast", "forlorn", "gloomy", "glum", "grief-stricken", "grieved",
    "heartbroken", "heartsick", "heavyhearted", "hurting", "languishing", "low",
    "low-spirited", "lugubrious", "melancholy", "morbid", "morose", "mournful",
    "pensive", "pessimistic", "somber", "sorrowful", "sorry", "troubled",
    "weeping", "wistful", "woebegone"]


def get_twitter_search_json(params):
    """
        Search the twitter Search API using the given @params
    """
    try:

        print TWITTER_SEARCH_URL % params

        # Open the Twitter search url, using the params
        res = urllib2.urlopen(TWITTER_SEARCH_URL % params)

        # convert to json and return
        return simplejson.loads(res.read())

    except urllib2.URLError:

        # We got a 404
        return False

    # Not catching everything yet so return False by default
    return False

def chunker(seq, size):
    """
        We can't send too much at once, so gotta chunk it up
    """
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def is_mood(text, mood_words):
    """
        If the text contains words for the given mood return true
    """
    for word in mood_words:
        if text.lower().find(word) > -1:
            return True
    return False

def get_moods(text):
    """
        Get a list of the moods for the text
    """

    moods = []

    # Happy
    if is_mood(text, HAPPY_WORDS):
        moods.append("happy")

    # Sad
    if is_mood(text, SAD_WORDS):
        moods.append("sad")

    return moods

def create_record(json, mood):
    """
        Create a SimpleGeo record from a mood and tweet
    """

    # get the coordinates
    coors = json['geo']['coordinates']

    # make a python timestamp
    timestamp = int(time.mktime(
        time.strptime(json['created_at'],
        "%a, %d %b %Y %H:%M:%S +0000")))

    # new record
    record = Record(
        layer=SIMPLE_GEO_LAYER,
        # making it unique by tweetid_mood
        id="%s_%s" % (json['id'], mood),
        lat=coors[0],
        lon=coors[1],
        created=timestamp,
        # storing the mood for searching
        mood=mood
    )

    return record

def get_records(results):
    """
        Get a list of records from the json results
    """

    # start with an empty list
    records = []

    # for each result in results
    for result in results:

        # get the geo info
        geo = result["geo"]

        # test if geo exists, and has coordinates
        if geo and geo["coordinates"]:

            # get the moods for the tweet
            moods = get_moods(result["text"])

            # for each mood
            for mood in moods:

                # create a mood record and append it to the list
                records.append(create_record(result, mood))

    return records

def prepare_mood_query():
    """
        Combine all of the mood words into a search query
    """

    happy = '+OR+'.join(HAPPY_WORDS)

    sad = '+OR+'.join(SAD_WORDS)

    return "q=%s+OR+%s" % (happy, sad)

def main():
    """
        Main mood searching def. This will ping the twitter search api
        every [SLEEP_TIME] seconds and find tweets that display a mood, and
        have a location
    """

    # Found it easier to store the parameters as a list
    # so I can update the since_id
    params_list = [
        "q=query",
        "rpp=100",
        "since_id=1"
    ]

    # Get the SimpleGeo Client
    client = Client(SIMPLE_GEO_TOKEN, SIMPLE_GEO_SECRET)

    # update the query to the mood words
    params_list[0] = prepare_mood_query()

    # Try block for Ctrl-C so we don't end with an exception
    try:

        while True:

            # Init an empty list of records
            records = []

            # Search twitter
            json = get_twitter_search_json('&'.join(params_list))

            # if we got something back
            if json:

                # store the results
                results = json["results"]

                # set the since_id so we don't search more than we need
                params_list[2] = "since_id=%s" % json['max_id']

                # if we got at least 1 result
                if len(results) > 0:

                    # get the SimpleGeo records from the api
                    records = get_records(results)
            else:

                print "API Error: No data returned"

            # Save 90 records at a time
            for chunk in chunker(records, 90):

                # add records to the SimpleGeo Layer
                client.add_records(SIMPLE_GEO_LAYER, chunk)

                # how many records added
                print "%s records added" % len(chunk)

            # Wait x seconds before continuing
            time.sleep(SLEEP_TIME)

    except KeyboardInterrupt:

        print 'Ctrl-C hit'

    # exit cleanly
    sys.exit()

if __name__ == '__main__':

    # hey ho, let's go
    main()