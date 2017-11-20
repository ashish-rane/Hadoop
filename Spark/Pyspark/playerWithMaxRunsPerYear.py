from pyspark import SparkConf,SparkContext

conf = SparkConf()
conf.setAppName("Max runs per year")

sc = SparkContext(conf=conf)

def parseBatting(rec):
	parts = rec.split(",")
	runs = 0
	if(len(parts[2]) != 0):
		runs = int(parts[2])
	return (parts[0], int(parts[1]), runs )

def topNRuns(values, num):
	runsList = sorted(set(map(lambda x: x[2], values)), reverse = True)[0:num]
	result = []
	for x in values:
		if(x[2] in runsList):
			result.append(x)

	return result

batting = sc.textFile("/user/cloudera/datafiles/common/lahman/batting.csv", False)

battingRec = batting.map(lambda x : parseBatting(x))
battingFilt = battingRec.filter(lambda x : x[2] > 0)
battingGrp = battingFilt.map(lambda x : (x[1], x))

'''
maxRunsPerYear = battingGrp.reduceByKey(lambda x, y: x if(x[2] >= y[2]) else y)

sorted = maxRunsPerYear.sortByKey()

for x in sorted.collect(): print(x)
'''
# Dense Ranking if two or more players have max runs
maxRunsPerYear = battingGrp.groupByKey().flatMapValues(lambda x : topNRuns(x, 1))

sorted = maxRunsPerYear.sortByKey()
for x in sorted.collect(): print(x)
