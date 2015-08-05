from pyspark import SparkContext

sc = SparkContext("local", "usersProfile")

rawData = sc.textFile("./fakeData*", 22)

parsedData = (rawData.map(lambda line: line.split("|"))
		     .map(lambda s: ((int(s[0]), int(s[3])), 1))) # UserID, TrackId, 1
					
# then we reduce by key (sum all ones, by grouping by (UserID, TrackId) key

redData = (parsedData.reduceByKey(lambda a,b: a+b)
		     .sortByKey())

redData.saveAsHadoopFile("profileMat", "org.apache.hadoop.mapred.TextOutputFormat",
                          compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
