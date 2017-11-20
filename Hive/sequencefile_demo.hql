-- Create table stored as sequence file
CREATE TABLE stocks_seq (exch string, symbol string, ymd string, open float, high float, low float, close float, volume int, adj_close float)
STORED AS SEQUENCEFILE;

-- Set the sequence file compression to BLOCK
SET mapred.output.compression.type = BLOCK;

INSERT OVERWRITE TABLE stocks_seq SELECT * FROM stocks;

-- Verify 
dfs -ls /apps/hive/warehouse/hirw.db/stocks_seq


SELECT * FROM stocks_seq LIMIT 10;
