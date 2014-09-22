from tweepy import API
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pdb import *
from tweepy.utils import import_simplejson as json
from time import sleep 
import sys

#Arun
ckey = "kJYFQA18ICfjquVkk3T1w"
csecret = "93DacQPIt97M9895E4tP4zw0EPj4r9SxKDn1MRmUyE"
atoken = "131783295-vMI4ydhdusaDoV3tL4oW7spYlppdHYk1WyljvNw7"
asecret = "I6wbG2NcjWrKiSTsFpI22hXpOHx2avVbOLvZu0dBCHd03"

"""
#palani
atoken = "2270676612-VWVHhAQ6O7ygsqJyCcjCMjlJVnKK44WCopwJNeO"
asecret = "ZBz8GivST2HYaWSutu6OZ71HNDxIhpYbbjriZ1zV3FaUW"
ckey = "KAZHpIkSVVgN4FcQ6Y3HCexaI"
csecret = "ONoHHHh9bvXdZJ1q2DHN0O003cxLamuhnC0rSoLGx7Ze1pQhzg"

"""
stopCount = 100000
start = 0

#loopId = 'parsripals88'
nextList = [str(sys.argv[1]).strip()]

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
api = API(auth)
f = open("follow_pals1.csv",'a')
while 1:
  try:  
    tempList = []
    for person in nextList:
        print person,"'s followers :\n"
        try :
            for fllwr in api.followers(person):
                print fllwr.screen_name, fllwr.followers_count
                f.write( '\n'+fllwr.screen_name+'\t'+ str(fllwr.followers_count))
                tempList.append(fllwr.screen_name)
        except Exception, e:
            print "Error : ", e
    nextList = tempList    
  except Exception, e:
    print "Error : ", e
    continue
     
