CREATE DATABASE hirw;

use hirw;

CREATE EXTERNAL TABLE stocks (exch string, symbol string, ymd string, open float, high float, low float, close float, volume int, adj_close float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://sandbox.hortonworks.com:8020/user/root/datafiles/hirw/stocks';


-- ORDER BY - Performance issue with only 1 reducer
SELECT * FROM stocks ORDER BY close DESC LIMIT 10;

-- first set number of reducers
SET mapreduce.job.reduces=3;

-- Even after set the num reducers to 3 the order by still uses 1 reducer.
SELECT * FROM stocks ORDER BY close DESC LIMIT 10;


-- SORT BY - Sorted per reducer but overall could be unordered. Also the records for the sort by column may be sent to 
-- different reducers. So the same symbol may be scattered across the output files.
INSERT OVERWRITE LOCAL DIRECTORY '/root/Projects/Hive/sort_by01/' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
SELECT ymd,symbol,close FROM stocks WHERE Year(ymd) = '2003' SORT BY symbol ASC, close DESC;

--VERIFY symbol KFT scattered across
cat /root/Projects/Hive/sort_by01/part-r-00000
cat /root/Projects/Hive/sort_by01/part-r-00001


-- DISTRIBUTE BY - Distributes the records to reducers based on this column. This means the following query will
-- ensure the records of same symbol will go to the same reducer. This basically says use this column value as reducer key.
INSERT OVERWRITE LOCAL DIRECTORY '/root/Projects/Hive/sort_dist_by01/' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
SELECT ymd,symbol,close FROM stocks WHERE Year(ymd) = '2003' DISTRIBUTE BY symbol  SORT BY symbol ASC, close DESC;

-- Verify Symbol KFT present only in 1 file.
cat /root/Projects/Hive/sort_dist_by01/part-r-00000
cat /root/Projects/Hive/sort_dist_by01/part-r-00001
cat /root/Projects/Hive/sort_dist_by01/part-r-00002


-- CLUSTER BY - If the column used in SORT BY and DISTRIBUTE BY is same then we can combine the sorting and distribution into cluster clause.
-- Following two queries are equivalent
INSERT OVERWRITE LOCAL DIRECTORY '/root/Projects/Hive/sort_dist_sym01/' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
SELECT ymd,symbol,close FROM stocks WHERE Year(ymd) = '2003' DISTRIBUTE BY symbol  SORT BY symbol ASC;

INSERT OVERWRITE LOCAL DIRECTORY '/root/Projects/Hive/cluster_by_sym01/' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
SELECT ymd,symbol,close FROM stocks WHERE Year(ymd) = '2003' CLUSTER BY symbol;






