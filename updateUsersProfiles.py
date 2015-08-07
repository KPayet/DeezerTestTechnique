''' updates users profile sparse matrix stored with not yet processed stream_xxxxxxxx files '''

import sys
import getopt
import os
from pyspark import SparkContext

'''same as in the previous script. Defined to add overwrite option'''
def saveAsTextFile(rdd, path, overwrite=True):
    if overwrite:
        os.system("rm -r " + path)

    rdd.saveAsTextFile(path, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

# Retrieves and deals with line arguments
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

    return matrixFiles, inputFiles, outputFile


def main(argv):
    
    ''' matrixDirectory: the hdfs directory where we find users profile matrix. It is assumed to be compressed 
                        and split in several files.
        streamFiles: the files used to update the matrix. In userId|country|artistId|trackId format
        outputFile: optional output directory for the updated matrix. By default, we simply overwrite the current one'''
    matrixDirectory, streamFiles, outputFile = getArguments(argv)

    sc = SparkContext(appName="usersProfile")
    
    # open both matrix and non processed stream_xxxxxxxx files
    # Turn into (key, value) pair, where key = (user, track), to prepare the join
    matrix = (sc.textFile(matrixDirectory + "*.gz")
                .map(lambda line: map(int, line.split(" ")))
                .map(lambda t: ((t[0], t[1]), t[2])))

    streamData = (sc.textFile(streamFiles)
                    .map(lambda line:  line.split("|"))
		    .map(lambda t: ((int(t[0]), int(t[3])), 1)))
  
  
    outData = (matrix.join(streamData) # here the entries look like ((user, track), [count, 1, 1 ...])
	             .map(lambda t: (t[0], sum(t[1])) ) # compute new count => ((user, track), new_count)
                     .sortByKey()                  
		     .map(lambda t: " ".join(map(str, (t[0][0], t[0][1], t[1]))))) # prepare output file

    saveAsTextFile(outData, path = outputFile, overwrite = True)

if __name__ == "__main__":
    main(sys.argv[1:])
