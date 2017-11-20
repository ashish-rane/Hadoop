from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName("Intersection Demo Pyspark")

sc = SparkContext(conf=conf)

colors1 = sc.parallelize(["Red", "Green", "Blue", "Cyan", "Magenta", "Yellow", "Orange"])
colors2 = sc.parallelize(["Yellow", "Blue", "Pink", "Red", "Violet", "Purple"])

colorsIntersect = colors1.intersection(colors2)

for x in colorsIntersect.collect(): print(x)
