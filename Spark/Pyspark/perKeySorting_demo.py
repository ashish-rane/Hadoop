
from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName("Sorting and ranking per key Pyspark")

sc = SparkContext(conf = conf)

# Sort the products by Price Descending

path = "hdfs://ip-172-31-53-48.ec2.internal:8020/apps/hive/warehouse/retail_db.db/products"


def parseProduct(rec):
	parts = rec.split(",")
	return (int(parts[0]), int(parts[1]), parts[2], parts[3], float(parts[4]))

products = sc.textFile(path).map(lambda x : parseProduct(x))


# Top N Products By Price By Category (sparse ranking)
# 1 . Frist Method using groupBy()
productsCatGrp = products.groupBy(lambda x : x[1])


# 2. Second Method using K,V with groupByKey()
productsCatGrp1 =  products.map(lambda x: (x[1], x)).groupByKey()

# Both the above methods return the same structure so it does not matter which one we use
top3ByPricePerCat = productsCatGrp.flatMapValues(lambda x: sorted(x, key= lambda i: i[4], reverse= True)[0:3])
for x in top3ByPricePerCat.collect(): print(x)

# Top N Priced Products per category (dense ranking)

def getDenseTopN(seq, n):
    # first get all prices only into a list
    priceList = map(lambda x: x[4], seq)
    #take distinct prices and sort them in desc order and take top3 
    top3Prices = set(sorted(set(priceList), reverse = True)[0:n])

    # Sort the original sequence by price desc
    sortedList = sorted(seq, key= lambda x: x[4], reverse = True)
    result = []
    # if any element in the sorted list is present in the top3Prices add to result 
    for x in sortedList:
        if(x[4] in top3Prices):
	    result.append(x)

    return result


top3PricedProducts = productsCatGrp.flatMapValues(lambda x: getDenseTopN(list(x), 3))
for x in top3PricedProducts.collect(): print(x)

