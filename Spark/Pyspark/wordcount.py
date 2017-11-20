from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName("WordCount")
#sc = SparkContext("local", "Wordcount")
sc = SparkContext(conf=conf)
book = sc.textFile("/user/cloudera/datafiles/common/book.txt").cache()

wordcount = book.flatMap(lambda line: line.split(" ")).map(lambda w: (w,1)).reduceByKey(lambda x,y: x + y)

# str() converts the unicode strings to normal strings
wcList = wordcount.map(lambda x: (str(x[0]), x[1])).collect()
print(wcList)

