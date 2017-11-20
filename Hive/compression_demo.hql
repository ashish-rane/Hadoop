-- enable compression for map and reduce phase.
-- Set engine to mr
SET hive.execution.engine=mr;
SET hive.exec.compress.intermediate = true;
SET mapred.map.output.compression.codec = org.apache.hadoop.io.compress.GZipCodec;
SET hive.exec.compress.output = true;
SET mapred.output.compression.codec = org.apache.hadoop.io.compress.GzipCodec;

use hirw;

-- Create the table using the compression settings
CREATE TABLE stocks_on_gz ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS SELECT * FROM stocks;

-- Verify files
dfs -ls /apps/hive/warehouse/hirw.db/stocks_on_gz


-- Verify reading from compressed table
SELECT * FROM stocks_on_gz LIMIT 10;
