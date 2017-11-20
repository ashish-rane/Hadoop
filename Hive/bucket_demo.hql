use hirw;

CREATE TABLE IF NOT EXISTS stocks_bucket (exchnge string, symbol string, ymd string, open float, high float, low float, close float, volume int, adj_close float)
PARTITIONED BY (exch string, yr string)
CLUSTERED BY (symbol) INTO 5 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


DESCRIBE FORMATTED stocks_bucket;

-- INSERT DATA INTO BUCKETED TABLE
-- Requires the following paramaters set
SET hive.enforce.bucketing = true;

SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=1000
SET hive.exec.max.dynamic.partitions.pernode=100;

INSERT OVERWRITE TABLE stocks_bucket PARTITION(exch='ABCSE',  yr)
SELECT s.*, Year(s.ymd) FROM stocks s WHERE Year(s.ymd) IN ('2001', '2002', '2003') AND s.symbol LIKE 'KG%';

-- Check the files
dfs -ls /apps/hive/warehouse/hirw.db/stocks_bucket/exch=ABCSE/yr=2001/

--Table sampling is more efficient

-- time consuming as table is not bucketed
SELECT * FROM stocks TABLESAMPLE(BUCKET 3 OUT OF 5 ON symbol) s;


-- More efficient sampling using bucketed table
SELECT * FROM stocks_bucket TABLESAMPLE(BUCKET 3 OUT OF 5 ON symbol) s;


-- SORTING WITH A BUCKET
CREATE TABLE IF NOT EXISTS stocks_sort_bucket (exchnge string, symbol string, ymd string, open float, high float, low float, close float, volume int, adj_close float)
PARTITIONED BY (exch string, yr string)
CLUSTERED BY (symbol) SORTED BY (volume DESC) INTO 5 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE stocks_sort_bucket PARTITION(exch='ABCSE',  yr)
SELECT s.*, Year(s.ymd) FROM stocks s WHERE Year(s.ymd) IN ('2001', '2002', '2003') AND s.symbol LIKE 'KG%'; 

-- Check the files
dfs -ls /apps/hive/warehouse/hirw.db/stocks_sort_bucket/exch=ABCSE/yr=2001/

