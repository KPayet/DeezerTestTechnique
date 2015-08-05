import os
from pyspark import SparkContext

def saveAsTextFile(rdd, path, overwrite=False):
    if overwrite:
        os.system("rm -r " + path)

    rdd.saveAsTextFile(path, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

sc = SparkContext("local", "usersProfile")

rawData = sc.textFile("./fakeData1M", 4)

outData = (rawData.map(lambda line: line.split("|"))
		     .filter(lambda s: int(s[0])<=10000)
		     .map(lambda t: ((int(t[0]), int(t[3])), 1))
		     .reduceByKey(lambda a,b: a+b)
		     .sortByKey()
		     .map(lambda t: " ".join(map(str, (t[0][0], t[0][1], t[1])))))

saveAsTextFile(outData, path = "./profileMat", overwrite = True)
