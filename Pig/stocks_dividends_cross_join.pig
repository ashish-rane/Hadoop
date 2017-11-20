stocks = LOAD '/user/hirw/input/stocks/' using PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float,high:float, low:float, close:float, volume:int, adj_close:float);

dividends = LOAD '/user/hirw/input/dividends' USING PigStorage(',') AS (exchange:chararray, symbol:chararray, date:datetime, dividend:float);

companies = LOAD '/user/hirw/input/companies' USING PigStorage(';') AS (symbol:chararray, name:chararray, address:map[]);


-- CROSS is used to make a join based on conditions other than equality. For eg: Joining stocks, dividends when symbol and date don't match. We use cross followed by filterting based on inequality
cross = CROSS stocks, dividends;

joined_non_eq = FILTER cross BY stocks::symbol != dividends::symbol;
