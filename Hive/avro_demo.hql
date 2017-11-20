
SET hive.execution.engine=mr;
SET hive.exec.compress.intermediate=true;
SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.GZipCodec;
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;

-- Create a avro table 
use hirw;

CREATE TABLE stocks_avro STORED AS AVRO TBLPROPERTIES('avro.schema.url'='hdfs://sandbox.hortonworks.com:8020/user/root/pigInputs/stocks.avsc');

INSERT OVERWRITE TABLE stocks_avro SELECT * FROM stocks;

-- Verify
dfs -ls /apps/hive/warehouse/hirw.db/stocks_avro

SELECT * FROM stocks_avro;
