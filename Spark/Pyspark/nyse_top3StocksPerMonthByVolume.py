from pyspark import SparkContext
from pyspark import SparkConf
from datetime import datetime

conf = SparkConf()
conf.setAppName("Top3 Stock By Vol Per Day")
sc = SparkContext(conf=conf)


def printRec(x): print(x)

def parseStockRec(rec):
    parts = rec.split(",")
    return (parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], int(parts[6]))


stocks = sc.textFile("/user/cloudera/datafiles/itversity/nyse/nyse_2014.csv", False)

stocksRec = stocks.map(lambda x: parseStockRec(x))

#stocksKV = stocksRec.map(lambda x: (x.strptime(x[1], "%d-%b-%Y").strftime("%Y-%m-%d"), x))

stocksGrp = stocksRec.groupBy(lambda x: datetime.strptime(x[1], "%d-%b-%Y").strftime("%Y-%m-%d"))

top3StocksByVol = stocksGrp.flatMapValues(lambda values: sorted(values, key = lambda x: x[6], reverse = True)[0:3])
sorted = top3StocksByVol.sortByKey()

for x in sorted.collect(): print(x)





