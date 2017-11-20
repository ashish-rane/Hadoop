from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName("Map Partitions Pyspark")
sc = SparkContext(conf=conf)

colors = sc.parallelize(["red", "green", "yellow","blue","pink", "black", "cyan", "orange"])

colorMap1  = colors.mapPartitions(lambda items : map(lambda x: x.upper(), items))


colorMap2 = colors.mapPartitionsWithIndex(lambda index, items: map(lambda x: x + "=>" + str(index), items))


for x in colorMap1.collect(): print(x)

for x in colorMap2.collect(): print(x)
