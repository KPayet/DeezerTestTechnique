import random
from pyspark import SparkContext

def getRandomPairList(x, N = 10):
    y = [(x, random.randint(1,35000000)) for i in range(1,11)]
    return y

sc = SparkContext(appName="makeLargeUtilMatrix")

X = xrange(1,15000000)

rdd = (sc.parallelize(X)
        .flatMap(lambda x: getRandomPairList(x, 200))
        .map(lambda t: ((t[0], t[1]), random.randint(1,100)))
        .map(lambda t: " ".join(map(str, (t[0][0], t[0][1], t[1])))))

rdd.saveAsTextFile("hdfs:///deezer/largeUtilMatrix" , compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")


