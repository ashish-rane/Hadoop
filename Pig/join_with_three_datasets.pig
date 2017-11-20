stocks = LOAD '/user/hirw/input/stocks/' using PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float,high:float, low:float, close:float, volume:int, adj_close:float);

dividends = LOAD '/user/hirw/input/dividends' USING PigStorage(',') AS (exchange:chararray, symbol:chararray, date:datetime, dividend:float);

companies = LOAD '/user/hirw/input/companies' USING PigStorage(';') AS (symbol:chararray, name:chararray, address:map[]);
cmp = FOREACH companies GENERATE symbol, name, address#'street', address#'city', address#'state';

join_multi = JOIN stocks BY symbol , dividends BY symbol, cmp BY symbol;

limit10 = LIMIT join_multi 10;

DUMP limit10;


