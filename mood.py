from datetime import date
from datetime import datetime
from datetime import timedelta

from const import SIMPLE_GEO_TOKEN, SIMPLE_GEO_SECRET, SIMPLE_GEO_LAYER

from django.utils import simplejson
from simplegeo import Record, Client

import os
import signal
import string
import sys
import threading
import sched, time

import urllib2

SLEEP_TIME = 5.0

TWITTER_SEARCH_URL = "http://search.twitter.com/search.json?%s"

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
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def is_happy(text):
    if text.lower().find("happy") > -1:
        return True
    return False

def is_sad(text):
    if text.lower().find("sad") > -1:
        return True
    return False

def get_moods(text):
    
    moods = []
    
    # Happy
    if is_happy(text):
        moods.append("happy")

    # Sad
    if is_sad(text):
        moods.append("sad")

    return moods

def create_record(json,mood):
    coors = json['geo']['coordinates']
    
    timestamp = int(time.mktime(
        time.strptime(json['created_at'],
        "%a, %d %b %Y %H:%M:%S +0000")))
        
    record = Record(
        layer=SIMPLE_GEO_LAYER,
        id="%s_%s" % (json['id'], mood),
        lat=coors[0],
        lon=coors[1],
        created=timestamp,
        mood=mood
    )
    return record

def get_records(results):

    records = []
    
    for result in results:
        geo = result["geo"]
        if geo and geo["coordinates"]:
            moods = get_moods(result["text"])
            for mood in moods:
                records.append(create_record(result, mood))
                
    return records

def im_moody():
    """
        
    """
    
    params_list = [
        "q=%22happy%22+OR+%22sad%22",
        "rpp=100",
        ""
    ]
    
    client = Client(SIMPLE_GEO_TOKEN, SIMPLE_GEO_SECRET)
    
    while True:
        
        records = []
        
        json = get_twitter_search_json('&'.join(params_list))
        
        if json:
            results = json["results"]
            params_list[2] = "since_id=%s" % json['max_id']
            if len(results) > 0:
                records = get_records(results)
        
        for chunk in chunker(records, 90):
            client.add_records(SIMPLE_GEO_LAYER, chunk)
            print "%s records added" % len(chunk)
        
        time.sleep(SLEEP_TIME)

class MyThread(threading.Thread):
    """
    This is a wrapper for threading.Thread that improves
    the syntax for creating and starting threads.
    """
    def __init__(self, target, *args):
        threading.Thread.__init__(self, target=target, args=args)
        print 'Initiating MyThread for %s ...' % target
        self.start()

        
def Process():
    child0 = MyThread(im_moody)
    child0.join()

# http://code.activestate.com/recipes/496735/
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
    """
    
    def __init__(self):
        """ Creates a child thread, which returns.  The parent
            thread waits for a KeyboardInterrupt and then kills
            the child thread.
        """
        self.child = os.fork()
        if self.child == 0:
            print 'continue onto the child process'
            return
        else:
            self.watch()

    def watch(self):
        """
        Parent process which waits for the child process to finish
        """
        try:
            os.wait()
        except KeyboardInterrupt:
            print 'Ctrl-C hit'
            self.kill()
        sys.exit()

    def kill(self):
        try:
            os.kill(self.child, signal.SIGKILL)
        except OSError:
            pass

        
    
def main():
    """
    main documentation
    """
    Watcher()
    Process()


if __name__ == '__main__':
    main()
