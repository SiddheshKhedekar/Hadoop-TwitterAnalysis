from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pdb import *
from tweepy.utils import import_simplejson as json 

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

def nothas2hash(s):
    return s.count('#') != 2

inp_keys = ['contributors', 'truncated', 'text', 'in_reply_to_status_id', 'id', 'favorite_count', 'source', 'retweeted', 'coordinates', 'entities', 'in_reply_to_screen_name', 'id_str', 'retweet_count', 'in_reply_to_user_id', 'favorited', 'user', 'geo', 'in_reply_to_user_id_str', 'possibly_sensitive', 'lang', 'created_at', 'filter_level', 'in_reply_to_status_id_str', 'place']

myfile = open("tweet_india9.csv", 'a')

class Listener(StreamListener):
    def on_data(self, data):
        pyData = json().loads(data)
        flag = 0
        try:
            if is_ascii(pyData['text']):
               flag = 1
               good+=1
               pyData['text'] = pyData['text'].replace('#', ' #')
               pyData['text'] = pyData['text'].replace('@', ' @')
               output_string = '  '.join(pyData['text'].split('\n')) + '\n'
        except: 
            pass
        if flag:
           print output_string
           myfile.write(output_string)
        return True
    def on_error(self, error):
        print "error: ",error
        return False

while 1:
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    twitterStream = Stream(auth, Listener())  
    ten_set = twitterStream.filter(track=["t20", "ipl", "csk", "dhoni","india","maharashtra", "andhra", "tamil", "tamilnadu", "delhi", "bengal", "mumbai" ,"chennai","uttar pradesh", "kerala","bengaluru", "bangalore","karnataka","goa","orissa","madhya", "nagaland", "meghalaya", "manipur", "punjab", "haryana", "himachal", "kashmir" ,"modi" , "kejriwal", "arvind", "narendra", "amma", "bjp", "congress" , "rahul", "admk" ,"dmk", "vijaykanth"])


