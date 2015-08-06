import sys
import getopt
import os
from pyspark import SparkContext

def saveAsTextFile(rdd, path, overwrite=False):
    if overwrite:
        os.system("rm -r " + path)

    rdd.saveAsTextFile(path, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

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

    inputFile, outputFile, nPart = getArguments(argv)

    sc = SparkContext(appName="usersProfile")
    rawData = sc.textFile(inputFile, nPart)

    outData = (rawData.map(lambda line: line.split("|"))
		      .map(lambda t: ((int(t[0]), int(t[3])), 1))
		      .reduceByKey(lambda a,b: a+b)
		      .sortByKey()
		      .map(lambda t: " ".join(map(str, (t[0][0], t[0][1], t[1])))))

    saveAsTextFile(outData, path = outputFile, overwrite = True)

if __name__ == "__main__":
    main(sys.argv[1:])
