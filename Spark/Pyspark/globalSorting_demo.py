
from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName("Sort Product By Price Pyspark")

sc = SparkContext(conf = conf)

# Sort the products by Price Descending

path = "hdfs://ip-172-31-53-48.ec2.internal:8020/apps/hive/warehouse/retail_db.db/products"


def parseProduct(rec):
	parts = rec.split(",")
	return (int(parts[0]), int(parts[1]), parts[2], parts[3], float(parts[4]))

products = sc.textFile(path).map(lambda x : parseProduct(x))

# 1. First Method: using sortByKey()
productsKV = products.map(lambda x : (x[4], x))

# ASC productsKv.sortByKey(true)
#priceDesc = productsKV.sortByKey(False)

#for x in priceDesc.take(10) : print(x)


# 2. Second Method: using sortBy()
#priceDesc1 = products.sortBy(lambda x : x[4], False)

#for x in priceDesc1.take(10) : print(x)


# SORTING WITH MULTIPLE FIELDS 
# sort by product price and then by product id in asc
# The -ve method works with numeric types only.

#using sortByKey()
#multiSort = products.map(lambda x: ((x[4], x[0]), x)).sortByKey(keyfunc = lambda x: (-x[0], x[1]))
#for x in multiSort.take(20): print(x)

#multiSort1 = products.sortBy(lambda x : (-x[4], x[0]))

#for x in multiSort1.take(20): print(x)

# for using String in ASC or desc use the optional parameter to sortByKey() or sortBy() for ordering the string field
# and then  use -ve sign for ordering the numeric field

# Sort by product_price desc and then by name ASC
stringsort = products.sortBy(lambda x : (-x[4], str(x[2])))
for x in stringsort.take(20) : print(x)

# Now reverse the ordering. The -ve sign means desc but because of false parameter it becaomes ascending.
stringsort1 = products.sortBy(lambda x: (-x[4], str(x[2])), False)
for x in stringsort1.take(20) : print(x)




