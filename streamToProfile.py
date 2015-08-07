''' Turns stream_xxxxxxxx files into sparse users profile matrix  '''

import sys
import getopt
import os
from pyspark import SparkContext

# A simple save function for my RDDs, where I add the overwrite option
def saveAsTextFile(rdd, path, overwrite=False):
    if overwrite:
        os.system("rm -r " + path)
    
    # Compression is enabled by default. Make it as option ?
    rdd.saveAsTextFile(path, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

# Retrieves and deals with line arguments
def getArguments(argv):
    
    inputFile = ""
    outputFile = ""
    nPart = 4
    
    try:
      options, args = getopt.getopt(argv,"hi:o:n:")
    except getopt.GetoptError:
      print 'streamToProfile.py -i <inputfile> -o <outputfile> -n <npartitions>'
      sys.exit(2)
    for option, arg in options:
      if option == '-h':
         print 'streamToProfile.py -i <inputfile> -o <outputfile> -n <npartitions>'
         sys.exit()
      elif option == "-i":
         inputFile = arg
      elif option == "-o":
         outputFile = arg
      elif option == "-n":
         nPart = int(arg)
      
    return inputFile, outputFile, nPart


def main(argv):
    
    ''' inputFile = a file (or files) in the stream_xxxxxxxx format: userId|country|artistId|trackId
        outputFile = the output directory for the users profiles matrix: userId trackId count. 
        With compression on by default the file is split in part_r000*.gz files
        nPart = minimal number of partitions to use for our rdd'''
    inputFile, outputFile, nPart = getArguments(argv)
    
    '''Init Spark, and retrieve our input files'''
    sc = SparkContext(appName="usersProfile")
    rawData = sc.textFile(inputFile, nPart)
    
    '''This is where the magic happens'''
    outData = (rawData.map(lambda line: line.split("|"))    #parse input files into (userId, country, artistId, trackId)
		      .map(lambda t: ((int(t[0]), int(t[3])), 1))   #set data type, project, and map each entry to (key, value) pair
		      .reduceByKey(lambda a,b: a+b)                 #for each key=(userId, trackId), sum all the ones => count
		      .sortByKey()                                  #sort the rdd. This could be made into an option
		      .map(lambda t: " ".join(map(str, (t[0][0], t[0][1], t[1]))))) # turn into a string for outputting

    saveAsTextFile(outData, path = outputFile, overwrite = True)

if __name__ == "__main__":
    main(sys.argv[1:])
