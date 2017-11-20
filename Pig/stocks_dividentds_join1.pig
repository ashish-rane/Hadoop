stocks = LOAD '/user/hirw/input/stocks/' using PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float,high:float, low:float, close:float, volume:int, adj_close:float);

dividends = LOAD '/user/hirw/input/dividends' USING PigStorage(',') AS (exchange:chararray, symbol:chararray, date:datetime, dividend:float);

companies = LOAD '/user/hirw/input/companies' USING PigStorage(';') AS (symbol:chararray, name:chararray, address:map[]);

join_inner = JOIN stocks by(symbol, date) LEFT OUTER, dividends BY (symbol, date);

join_proj = FOREACH join_inner GENERATE stocks::symbol, dividends::date, dividends::dividend;

DUMP join_proj;
