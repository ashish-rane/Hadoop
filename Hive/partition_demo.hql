use hirw;

CREATE TABLE stocks_partitioned (exch string, symbol string, ymd string, open float, high float, low float, close float, volume int, adj_close float) 
PARTITIONED BY (sym string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- LOADING DATA INTO A SINGLE STATIC PARTITION USING INSERT...SELECT..
INSERT OVERWRITE TABLE stocks_partitioned PARTITION (sym = 'KI')
SELECT * FROM stocks WHERE symbol = 'KI';

-- check partitions on table
SHOW PARTITIONS stocks_partitioned;

-- EXTRACT ONLY 'KTT' records in a directory
INSERT OVERWRITE DIRECTORY '/user/root/output/hive/hirw/stocks_partitioned/ktt'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM stocks WHERE symbol = 'KTT';

-- add partition sym=KTT which points to the above location
ALTER TABLE stocks_partitioned ADD IF NOT EXISTS PARTITION (sym = 'KTT') LOCATION '/user/root/output/hive/hirw/stocks_partitioned/ktt';

dfs -ls /apps/hive/warehouse/hirw.db/stocks_partitioned/

-- CREATE MULITPLE PARTITIONS IN ONE STATEMENT
FROM stocks s
INSERT OVERWRITE TABLE stocks_partitioned PARTITION(sym = 'KD')
SELECT s.* WHERE s.symbol = 'KD'
INSERT OVERWRITE TABLE stocks_partitioned PARTITION(sym = 'KE')
SELECT s.* WHERE s.symbol = 'KE';

SHOW PARTITIONS stocks_partitioned;
dfs -ls /apps/hive/warehouse/hirw.db/stocks_partitioned/

-- DROPPING PARTITION
ALTER TABLE stocks_partitioned DROP IF EXISTS PARTITION(sym = 'KTT');

SHOW PARTITIONS stocks_partitioned;
-- Check if the external directory for partition KTT is deleted.
dfs -ls /user/root/output/hive/hirw/stocks_partitioned/


-- ENABLE DYNAMIC PARTITION
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=strict;

-- This wont work since mode is strict and requires atleast 1 static partition.
INSERT INTO stocks_partitioned SELECT s.*, s.symbol FROM stocks;

-- CREATE A TABLE with more than one partition column
CREATE TABLE IF NOT EXISTS stocks_dyn_partition (exchng string, symbol string, ymd string, open float, high float, low float, close float, volume int, adj_close float)
PARTITIONED BY (exch string, yr string, sym string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',';

DESCRIBE FORMATTED stocks_dyn_partition;

-- We are restricting the number of partitions created because of the following properties limit it
-- SET hive.exec.max.dynamic.partitions=1000;
-- SET hive.exec.max.dynamic.partitions.pernode=100;

INSERT OVERWRITE TABLE stocks_dyn_partition PARTITION(exch='ABCSE', yr, sym)
SELECT s.*, YEAR(s.ymd), s.symbol FROM stocks s WHERE YEAR(s.ymd) IN ('2001','2002','2003') AND s.symbol LIKE 'KG%';

SHOW PARTITIONS stocks_dyn_partition;

-- Check Directory structure
dfs -ls /apps/hive/warehouse/hirw.db/stocks_dyn_partition;

dfs -ls /apps/hive/warehouse/hirw.db/stocks_dyn_partition/exch=ABCSE;

dfs -ls /apps/hive/warehouse/hirw.db/stocks_dyn_partition/exch=ABCSE/yr=2001;

dfs -ls /apps/hive/warehouse/hirw.db/stocks_dyn_partition/exch=ABCSE/yr=2001/sym=KGC;

-- Selecting targeted partitions


SELECT * from stocks_dyn_partition WHERE yr=2003 AND volume > 10000;

SELECT * from stocks_dyn_partition WHERE yr=2003 AND sym='KGC' AND volume > 10000;

-- The following queries won't work since the property is strict which means the query to a partitioned table
-- should have a where clause with alteast one partition column
-- SET hive.mapred.mode=strict
SELECT * from stocks_dyn_partition WHERE volume > 10000;
SELECT * FROM stocks_dyn_partition;

-- But the following would work since we included the 'exch' partition column in where clause
SELECT * FROM stocks_dyn_partition WHERE exch='ABCSE' AND volume > 10000;




