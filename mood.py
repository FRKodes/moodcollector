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
import threading
import os, signal, operator


HAPPY_WORDS = ["happy", "blessed", "blest", "blissful", "blithe",
    "captivated", "cheerful", "chipper", "chirpy", "content", "contented",
    "convivial", "delighted", "ecstatic", "elated", "exultant", "glad",
    "gleeful", "gratified", "intoxicated", "jolly", "joyful", "joyous",
    "jubilant", "laughing", "light", "lively", "merry", "mirthful",
    "overjoyed", "peaceful", "peppy", "perky", "playful", "pleasant", "pleased",
    "sparkling", "sunny", "thrilled", "tickled", "upbeat"]

SAD_WORDS = ["sad", "bereaved", "bitter", "cheerless", "dejected", "despairing",
    "despondent", "disconsolate", "dismal", "distressed", "doleful", "down",
    "downcast", "forlorn", "gloomy", "glum", "grief-stricken", "grieved",
    "heartbroken", "heartsick", "heavyhearted", "hurting", "languishing", "low",
    "low-spirited", "lugubrious", "melancholy", "morbid", "morose", "mournful",
    "pensive", "pessimistic", "somber", "sorrowful", "sorry", "troubled",
    "weeping", "wistful", "woebegone"]

ANGRY_WORDS = ["angry", "affronted", "annoyed", "antagonized", "bitter",
    "chafed", "choleric", "convulsed", "cross", "displeased", "enraged",
    "exacerbated", "exasperated", "ferocious", "fierce", "fiery", "fuming",
    "furious", "galled", "hateful", "heated", "hot", "huffy", "impassioned",
    "incensed", "indignant", "inflamed", "infuriated", "irascible", "irate",
    "ireful", "irritable", "irritated", "maddened", "nettled", "offended",
    "outraged", "piqued", "provoked", "raging", "resentful", "riled", "sore",
    "splenetic", "storming", "sulky", "sullen", "turbulent", "uptight",
    "vexed", "wrathful"]

def get_twitter_search_json(params):
    """
        Search the twitter Search API using the given @params
    """
    try:

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

    # Angry
    if is_mood(text, ANGRY_WORDS):
        moods.append("angry")

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

def im_moody(mood_words):
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
    params_list[0] = "q=%s" % '+OR+'.join(mood_words)

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
            print "%s %s records added" % (len(chunk), mood_words[0])

        # Wait x seconds before continuing
        time.sleep(SLEEP_TIME)


class Watcher:
    """this class solves two problems with multithreaded
    programs in Python, (1) a signal might be delivered
    to any thread (which is just a malfeature) and (2) if
    the thread that gets the signal is waiting, the signal
    is ignored (which is a bug).

    The watcher is a concurrent process (not thread) that
    waits for a signal and the process that contains the
    threads.  See Appendix A of The Little Book of Semaphores.
    http://greenteapress.com/semaphores/

    I have only tested this on Linux.  I would expect it to
    work on the Macintosh and not work on Windows.
    
    Watcher taken from http://code.activestate.com/recipes/496735/
    """
    
    def __init__(self):
        """ Creates a child thread, which returns.  The parent
            thread waits for a KeyboardInterrupt and then kills
            the child thread.
        """
        self.child = os.fork()
        if self.child == 0:
            return
        else:
            self.watch()

    def watch(self):
        try:
            os.wait()
        except KeyboardInterrupt:
            # I put the capital B in KeyBoardInterrupt so I can
            # tell when the Watcher gets the SIGINT
            print 'KeyBoardInterrupt'
            self.kill()
        sys.exit()

    def kill(self):
        try:
            os.kill(self.child, signal.SIGKILL)
        except OSError: pass

class MyThread(threading.Thread):
    """
    This is a wrapper for threading.Thread that improves
    the syntax for creating and starting threads.
    """
    def __init__(self, target, *args):
        threading.Thread.__init__(self, target=target, args=args)
        self.start()

if __name__ == '__main__':
    
    # Python threading doesn't have a good ctrl-c keyboardinterrupt,
    # so using code from http://code.activestate.com/recipes/496735/
    Watcher()
    
    # hey ho, let's go
    MyThread(im_moody, HAPPY_WORDS)
    MyThread(im_moody, SAD_WORDS)
    MyThread(im_moody, ANGRY_WORDS)
