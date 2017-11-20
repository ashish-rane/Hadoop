from pyspark import SparkContext
from pyspark import SparkConf
import datetime

def printRec(x): print(x)

def parseStockRec(rec):
    parts = rec.split(",")
    sym = parts[0]
    trade_date =  datetime.datetime.strptime(parts[1], "%d-%b-%Y").strftime("%Y-%m")
    volume = int(parts[6])
    return ((sym, trade_date), volume)

#conf = SparkConf().setAppName("NYSE AvgStockVolPerMonth")
sc = SparkContext("local","NYSE AvgStockVolPerMonth")

stocks = sc.textFile("/user/cloudera/datafiles/itversity/nyse/nyse_2014.csv")

stocksRec = stocks.map(lambda x: parseStockRec(x))

stockPerMonthAgg =  stocksRec.aggregateByKey((0.0,0), lambda acc, val: (acc[0] + val, acc[1] + 1), lambda acc,value : (acc[0] + val[0], acc[1] + val[1]))

avgStockVolPerMonth = stockPerMonthAgg.map(lambda x: (x[0][1],  str(x[0][0]),  int(x[1][0]/x[1][1])))
sorted = avgStockVolPerMonth.sortBy(lambda x : (x[0], x[1]))
stockList = sorted.collect()
for x in stockList: print x
sorted.saveAsTextFile("/user/cloudera/output/itversity/spark/python/nyse_avgstockVolPerMonth")


