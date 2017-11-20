stocks = LOAD '/user/hirw/input/stocks/' using PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float,high:float, low:float, close:float, volume:int, adj_close:float);

dividends = LOAD '/user/hirw/input/dividends' USING PigStorage(',') AS (exchange:chararray, symbol:chararray, date:datetime, dividend:float);

companies = LOAD '/user/hirw/input/companies' USING PigStorage(';') AS (symbol:chararray, name:chararray, address:map[]);
cmp = FOREACH companies GENERATE symbol, name, address#'street', address#'city', address#'state';

cogrouped = COGROUP stocks BY (symbol, date), dividends BY (symbol, date);

simulated_inner_join =  FILTER cogrouped BY (NOT IsEmpty(stocks)) AND (NOT IsEmpty(dividends));

flattened = FOREACH simulated_inner_join GENERATE flatten(group), flatten(stocks), flatten(dividends);

limit10 = LIMIT flattened 10;

DUMP limit10;
