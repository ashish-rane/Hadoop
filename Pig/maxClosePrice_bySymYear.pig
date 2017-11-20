stocks = LOAD 'input/stocks/' using PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float,high:float, low:float, close:float, volume:int, adj_close:float);


grp_sym_year = GROUP stocks BY (symbol, GetYear(date));


max_close = FOREACH grp_sym_year GENERATE flatten(group), MAX(stocks.close) as maxClose;

STORE max_close INTO 'output/pig/hirw/maxCloseBySymYear02' USING PigStorage(',');
