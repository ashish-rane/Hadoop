-- Need to set the following properties to enable compression

-- Compress map output (also called intermediate output)
SET mapred.compress.map.output true;

-- compress final job output
SET mapred.output.compress true;

-- optionally specify the compression codec.
SET mapred.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;

stocks = LOAD '/user/root/datafiles/hirw/stocks' USING PigStorage(',') AS (exch:chararray, symbol:chararray, ymd:chararray, open:float, high:float, low:float, close:float, volume:int, adj_close:float);

-- Store the output using compression
STORE stocks INTO '/user/root/output/hirw/pig/stocks_compressed' USING PigStorage(',');

-- Verify compressed file
hadoop fs -ls output/hirw/pig/stocks_compressed/


-- Reload the stocks from the compressed storage
stocks01 = LOAD '/user/root/output/hirw/pig/stocks_compressed' USING PigStorage(',') AS (exch:chararray, symbol:chararray, ymd:chararray, open:float, high:float, low:float, close:float, volume:int, adj_close:float);

limit10 = LIMIT stocks01 10;
