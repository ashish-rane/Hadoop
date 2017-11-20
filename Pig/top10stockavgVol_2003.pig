stocks = LOAD 'input/stocks/' using PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float,high:float, low:float, close:float, volume:int, adj_close:float);

stocks_2003 = FILTER stocks by GetYear(date) == 2003;
grp_stock = GROUP stocks_2003 by symbol;

stock_avgVol = FOREACH grp_stock GENERATE group as symbol, ROUND(AVG(stocks_2003.volume)) as avgVol;

stock_sorted = ORDER stock_avgVol BY avgVol DESC;

stock_top10 = LIMIT  stock_sorted 10;

DUMP stock_top10;
