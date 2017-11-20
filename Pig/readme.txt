HDFS input location
Relative to user
mapreduceInputs

HDFS output location - Relative to user
output/pig/stocks

Delete output directory
hadoop fs -rm -r output/pig/stocks

Submit Pig script
pig Projects/Pig/MaxClosePrice.pig

View Results
hadoop fs -cat output/pig/stocks/part-r-00000



