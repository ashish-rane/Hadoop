
from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName("Sort Product By Price Pyspark")

sc = SparkContext(conf = conf)

# Sort the products by Price Descending

path = "hdfs://ip-172-31-53-48.ec2.internal:8020/apps/hive/warehouse/retail_db.db/products"


def parseProduct(rec):
	parts = rec.split(",")
	return (int(parts[0]), int(parts[1]), parts[2], parts[3], float(parts[4]))

def printRDD(rec) :
	for x in rec:
		print(x)

products = sc.textFile(path).map(lambda x : parseProduct(x))

# We could use sortByKey() or sortBy() for ranking too by using it in conjunction with take() to select only desired number of records,
# but for ranking we use either top if we need descending or takeOrdered() if we need ascending. 
# In both cases we can supply keyfunc  which specifies which columns to sort on. 
# (we can specify multiple fields as a tuple with -ve sign for numeric types in case we required mixed sorting)

# Get top 5 priced products
top5price = products.top(5, key = lambda x: x[4]) 
printRDD(top5price)
# OR using K,V
top5price1 = products.map(lambda x: (x[4], x)).top(5)
printRDD(top5price1)

# Get bottom 5 priced products
bottom5price = products.takeOrdered(5, key = lambda x: x[4])
printRDD(bottom5price)
# OR using K,V
bottom5price1 = products.map(lambda x : (x[4], x)).takeOrdered(5)
printRDD(bottom5price1)

# MULTI COLUMN RANKING. Say Top 3 Priced Products per category. Here event sort by category which is actually not needed.
# Also this will actually return only 3 results and not 3 results per category so its pretty much useless.
top3prodpercat = products.takeOrdered(3, key = lambda x : (x[1],-x[4]))
printRDD(top3prodpercat)




