import sys
import getopt
import os
from pyspark import SparkContext

def saveAsTextFile(rdd, path, overwrite=True):
    if overwrite:
        os.system("rm -r " + path)
    print path    
    rdd.saveAsTextFile(path, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

def getArguments(argv):
    
    matrixFiles = ""
    inputFiles = ""
    outputFile = ""
    
    try:
      options, args = getopt.getopt(argv,"hm:s:o:")
    except getopt.GetoptError:
      print 'updateUsersProfiles.py -m <matrixDirectory> -s <inputFiles> -o <outputFile>'
      sys.exit(2)
    for option, arg in options:
      if option == '-h':
         print 'streamToProfile.py -m <matrixDirectory> -s <inputFiles> -o <outputFile>'
         sys.exit()
      elif option == "-m":
         if arg[len(arg)-1] == "/":
           matrixFiles = arg
           outputFile = arg[:-1]
         else:
           matrixFiles = arg + "/"
           outputFile = arg
      elif option == "-s":
         inputFiles = arg
      elif option == "-o":
         outputFile = arg
    print matrixFiles, inputFiles, outputFile      
    return matrixFiles, inputFiles, outputFile


def main(argv):

    matrixDirectory, streamFiles, outputFile = getArguments(argv)

    sc = SparkContext(appName="usersProfile")

    matrix = (sc.textFile(matrixDirectory + "*.gz")
                .map(lambda line: map(int, line.split(" ")))
                .map(lambda t: ((t[0], t[1]), t[2])))

    streamData = (sc.textFile(streamFiles)
                    .map(lambda line:  line.split("|"))
		    .map(lambda t: ((int(t[0]), int(t[3])), 1)))
   
      
 
    outData = (matrix.join(streamData)
	             .map(lambda t: (t[0], sum(t[1])) )
                     .sortByKey()
		     .map(lambda t: " ".join(map(str, (t[0][0], t[0][1], t[1])))))

    saveAsTextFile(outData, path = outputFile, overwrite = True)

if __name__ == "__main__":
    main(sys.argv[1:])
