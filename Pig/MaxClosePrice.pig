--Load dataset with column name and datatypes
stock_records = LOAD 'mapreduceInputs' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, current:float, open:float,close:float,high:float,low:float,volume:int);

-- Group records by symbol
grp_by_sym = GROUP stock_records BY symbol;

-- Calculate maximum closing price
max_closing = FOREACH grp_by_sym GENERATE group, MAX(stock_records.close) as maxclose;


--Store output 
--STORE max_closing INTO 'output/pig/stocks' using PigStorage(',');
--ILLUSTRATE max_closing;
DUMP max_closing;
--DESCRIBE max_closing;
--EXPLAIN max_closing;
