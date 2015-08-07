import sys
import getopt
from pyspark import SparkContext

def getArguments(argv):
    
    userId = 1

    try:
      options, args = getopt.getopt(argv,"hu:")
    except getopt.GetoptError:
      print 'streamToProfile.py -u <user_id>'
      sys.exit(2)
    for option, arg in options:
      if option == '-h':
         print 'streamToProfile.py -u <user_id>'
         sys.exit()
      elif option == "-u":
         userId = int(arg)

    return userId

def test(user1, userId):
   return user1==userId

# la 
def getSimilarUsers(userId, rdd):
   
    top20 = (rdd.map(lambda line: line.split(" "))  # parse from line to tuple
                .map(lambda t: (int(t[0]), int(t[1]), float(t[2])))
		.filter(lambda t: t[0] == userId)       #t[0] = user1, t[1] = user2, t[2] = sim(user1, user2)
                .sortBy(lambda t: -1*t[2])          # we sort by descending order of similarity (t[2])
                .map(lambda t: t[1])
                .take(20)) # return as a list
                
    return top20
    
if __name__ == "__main__":
    
    userId = getArguments(sys.argv[1:])
    
    # init Spark, and retrieve similarityMatrix.
    # I suppose this one is stored on hdfs, and compressed.
    sc = SparkContext(appName = "top20Similarusers")
    simMatrix = sc.textFile("./similarityMatrix")    

    top20 = getSimilarUsers(userId, simMatrix)
    
    print "\n The 20 users most similar to user ", userId, " are: ", top20, "\n"
